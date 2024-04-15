import pandas as pd

#This is an additional file used for handling dataframes between the different contexts used by streamlit

__frames = dict()
__rows = dict()

# Get the dataframe for the appropriate company (or make one if missing)
def get_frame(ticker):
    df = __frames.get(ticker)
    if df is None:
        df = pd.DataFrame({
            "Topic": pd.Series(dtype="str"),
            "Open": pd.Series(dtype="float"),
            "High": pd.Series(dtype="float"),
            "Low": pd.Series(dtype="float"),
            "Close": pd.Series(dtype="float"),
            "Volume": pd.Series(dtype="float"),
            "Date": pd.Series(dtype="datetime64[s]")
        })
        __frames[ticker] = df
        __rows[ticker] = 0
    return df 

# Handle a received event and update the right dataframe for it.
def update(msg):
    send = dict()
    topic = msg.topic()

    send["Topic"] = [topic]

    send["Date"] = [pd.to_datetime(msg.value().decode('utf-8'), format="ISO8601")]

    data = msg.key().decode('utf-8').split(",")

    for attribute in data:
        field, value = attribute.split(":")
        send[field] = [float(value)]

    df = get_frame(topic)

    row = __rows[topic]
    new_df = pd.DataFrame(send, columns=df.columns, index=[row])
    __rows[topic] = row + 1

    df = pd.concat([df, new_df], ignore_index=True)
    if len(df.index) > 604800: # Up to a week of storage, assuming every interval is a second.
        df = df.drop(index=df.index.min())
    __frames[topic] = df