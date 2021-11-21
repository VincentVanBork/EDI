import pandas as pd

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
more_than_one_site = grouped_by_host.filter(lambda x: len(x) > 1)
print(len(more_than_one_site.groupby(by=["host"])))

grouped_more_action = more_than_one_site.groupby(by=["host"])

divided_by_diff = (
    grouped_more_action["time"]
    .transform(
        lambda x: x.diff(),
    )
    .fillna(0)
)
more_than_one_site["difference"] = divided_by_diff

more_than_one_site["session_typical_change"] = [
    True if diff > 30*60 else False for diff in more_than_one_site["difference"]
]
