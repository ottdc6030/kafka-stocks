#!/usr/bin/env python
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import time
import yfinance as yf

#The file for running a producer

def get_latest_stock_price(stock):
    data = stock.history()
    latest_stock_price = data['Close'].iloc[-1]
    return latest_stock_price

def get_today():
    return date.today()

def get_next_day_str(today):
    return get_date_from_string(today) + timedelta(days=1)

def get_date_from_string(expiration_date):
    return datetime.strptime(expiration_date, "%Y-%m-%d").date()

def get_string_from_date(expiration_date):
    return expiration_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('ticker', default='QQQ')
    parser.add_argument('--fromDate')
    parser.add_argument('--toDate')
    parser.add_argument('--drip', action="store_true") #Indicates if historic events should be gradually fed (true), or sent all at once (false)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # read the ticker
    ticker = args.ticker
    print('ticker', ticker)

    # Create Producer instance
    producer = Producer(config)

    # Parse search boundaries if necessary
    from_date = None if args.fromDate is None else pd.to_datetime(args.fromDate + " 00:00", format="%Y-%m-%d %H:%M")
    to_date = None if args.fromDate is None or args.toDate is None else pd.to_datetime(args.toDate + " 23:59" , format="%Y-%m-%d %H:%M")

    if from_date is not None and to_date is None:
        to_date = from_date + np.timedelta64(24, "H")

    if from_date is None:
        df = None
        real_time = True
    else:
        df = yf.download(tickers=ticker, start=from_date, end=to_date, interval="1m")
        real_time = False

    drip = args.drip


    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    count = 0
    # Produce data by repeatedly fetching today's stock prices - feel free to change

    topic = ticker
    ticker = yf.Ticker(ticker)

    most_recent_time = None

    # Serialize an event of new data and send it through the producer
    def parse_row(row):
        global most_recent_time
        datestr = str(row.name)

        row_time = pd.to_datetime(datestr, format="ISO8601")

        #Only send if new event
        if most_recent_time is not None and most_recent_time >= row_time: return
        most_recent_time = row_time

        data = []
        for column in ["Open", "High", "Low", "Close", "Volume"]:
            data.append(":".join([column, str(row[column])]))
        producer.produce(topic, datestr, ",".join(data), callback=delivery_callback)

    # Return the next set of data, be it from real-time or a pre-loaded dataframe.
    def more_data(): 
        if real_time: return ticker.history(period="1m")
        global df
        if drip: #Simulate real-time gradual information
            send = df.iloc[[0]]
            df = df.iloc[1:]
        else: #Dump everything at once
            send = df
            df = df.iloc[0:0]
        return send

    while real_time or df.index.size > 0:

        data = more_data()

        data.apply(parse_row, axis="columns")

        count += 1
        time.sleep(5)

        # Block until the messages are sent.
        producer.poll(10)
        producer.flush()
