import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.tseries.offsets import MonthEnd

# read the file
print("Hello")
df = pd.read_csv("./data.csv", index_col="index")
print(df.head())

# removing all not needed GET requests
remove_media = df[
    df.url.str.contains(".html", case=False) & df.url.str.contains(".htm", case=False)
    | df.url.str.endswith("/")
]
# only 200
print(remove_media["response"].unique())
remove_media = remove_media[remove_media["response"] == 200]
print(remove_media["response"].unique())

# grouping by host (client user)
grouped_by_host = remove_media.groupby(by=["host"])
print(len(grouped_by_host))

# removing session with only one action
data = grouped_by_host.filter(lambda x: len(x) > 1)
print(len(data.groupby(by=["host"])))

grouped_more_action = data.groupby(by=["host"])

divided_by_diff = (
    grouped_more_action["time"]
    .transform(
        lambda x: x.diff(),
    )
    .fillna(0)
)
data["difference"] = divided_by_diff

data["session_change"] = [
    True if diff > 30 * 60 else False for diff in data["difference"]
]
data["time_stamp"] = pd.to_datetime(data["time"], unit="s")

grouped_user_session = data.groupby(by=["host", pd.Grouper(key="time_stamp", freq="30min")])
data["session"] = grouped_user_session.ngroup()

data = data.groupby(by=["session"]).filter(lambda x: len(x) > 1)
print(data.head(30))
