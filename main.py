import pandas as pd
print("Hello")
df = pd.read_csv("./data.csv", index_col="index")
print(df.head())

remove_media = df[
    df.url.str.contains(".html", case=False) &
    df.url.str.contains(".htm", case=False) |
    df.url.str.endswith("/")
]


print(remove_media.head())
print(remove_media.host.unique())
# print(remove_media.host.unique())


# print(remove_media.url.unique())
