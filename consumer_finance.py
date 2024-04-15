#!/usr/bin/env python  
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import streamlit as st
import plotly.express as px

from consumer_data import get_frame, update


# Main file for running a consumer

#Return the value and the delta (relative to the beginning and end) of a column
def value_and_delta(column):
    if column.name == "Topic":
        return None
    value = column.iloc[-1]
    return {
        "value": value,
        "delta": 0.0 if len(column) < 2 else value - column.iloc[0]
    }

#Apply the current state of the data frame to the UI
def refresh_data(ticker, feed):
    df = get_frame(ticker)
    if df.index.size == 0:
        return
    with feed.container():
        company, *rest = st.columns(6)

        if df.index.size < 1:
            return
        
        data_sets = df.apply(value_and_delta)

        columns = ["Open", "High", "Low", "Close", "Volume"]

        company.metric(label="Company", value=ticker)

        for i in range(len(columns)):
            element =  rest[i]
            name = columns[i]
            data = data_sets[name]
            element.metric(label=name, value=data["value"],delta=data["delta"])

        numbers, volume = st.columns(2)

        with numbers:
            st.markdown("Stats")
            st.write(px.line(df, x="Date", y=columns[:-1]))

        with volume:
            st.markdown("Volume")
            st.write(px.line(df, x="Date", y="Volume"))



if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('ticker', action='append', nargs="+")
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    tickers = set(*args.ticker)
    tickers = [*tickers]
    print('tickers', tickers)

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    consumer.subscribe(tickers, on_assign=reset_offset)

    st.set_page_config(page_title="My Trackers", layout="wide")
    st.title("My Trackers")

    feed = st.empty()

    ticker = st.selectbox("Select Tracker", tickers)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            #print('msg', msg)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                #print("Waiting...")
                refresh_data(ticker, feed)
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                update(msg)
                refresh_data(ticker, feed)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
