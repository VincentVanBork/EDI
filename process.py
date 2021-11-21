import pandas as pd
import arff


# read the file
print("Hello")
df = pd.read_csv("./data.csv", index_col="index")
print(df.head())

# removing all not needed GET requests
remove_media = df[
    df.url.str.contains(".html", case=False) & df.url.str.contains(".htm", case=False)
    | df.url.str.endswith("/")
]
print("==================================")
# only 200
print(remove_media["response"].unique())
remove_media = remove_media[remove_media["response"] == 200]
print(remove_media["response"].unique())
print("==================================")
print(remove_media["method"].unique())
remove_media = remove_media[remove_media["method"] == "GET"]
print(remove_media["method"].unique())

print("==================================")
# grouping by host (client user)
grouped_by_host = remove_media.groupby(by=["host"])
print(len(grouped_by_host))
# removing session with only one action
data = grouped_by_host.filter(lambda x: len(x) > 1)
print(len(data.groupby(by=["host"])))
print("==================================")
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

grouped_user_session = data.groupby(
    by=["host", pd.Grouper(key="time_stamp", freq="30min")]
)
data["session"] = grouped_user_session.ngroup()

grouped_by_session = data.groupby(by=["session"])
data = grouped_by_session.filter(lambda x: len(x) > 1)

data["session_actions"] = data.groupby(by=["session"])["session"].transform(
    lambda x: len(x)
)

print("==================================")

data = data.groupby(by=["session"]).filter(lambda x: len(x) > 1)
data["session_time"] = data.groupby(by=["session"])["time"].transform(
    lambda x: x.iloc[-1] - x.iloc[0]
)

print("==================================")

data["session_average_per_page"] = data.groupby(by=["session"])[
    "session_time"
].transform(lambda x: x / x.count())

print("==================================")

print(data.url.unique())
max_length = data["time"].count()
data = data.groupby(by=["url"]).filter(
    lambda x: (x["time"].count() / max_length) >= 0.005
)
print(data.url.unique())

print("==================================")

for site in data.url.unique():
    # print(site)
    data[site] = data.groupby(by=["session"])["url"].transform(lambda x: site == x)
    # print(data[site].unique())

data.drop("method", inplace=True, axis=1)
data.drop("time_stamp", inplace=True, axis=1)
data.drop("response", inplace=True, axis=1)
# print(data.head(45))
print(data.info())

print("CREATING ARFF")
chosen_pages = data.url.unique()
sessions = data.drop(
    ["time", "host", "url", "bytes", "difference", "session_change"], axis=1
)
print(sessions.head())

grouped_sessions = sessions.groupby(by=["session"])
unique_sessions = pd.DataFrame()

s = []
t = []
a = []
p = []
all_sites = {}
for site in chosen_pages:
    all_sites[site] = []


for _, group in grouped_sessions:
    s.append(group["session"].unique()[0])
    a.append(group["session_actions"].unique()[0])
    t.append(group["session_time"].unique()[0])
    p.append(group["session_average_per_page"].unique()[0])
    for site_name in all_sites:
        all_sites[site_name] = any(group[site_name].unique())

unique_sessions["Session"] = s
unique_sessions["Actions"] = a
unique_sessions["Time"] = t
unique_sessions["PageAverageTime"] = p


for site_key in all_sites:
    unique_sessions[site_key] = all_sites[site_key]

print(unique_sessions.head())
print(unique_sessions.count())
arff.dump(
    "sessions.arff",
    unique_sessions.values,
    relation="sessions",
    names=unique_sessions.columns,
)
