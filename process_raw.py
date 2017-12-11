import pandas as pd
import numpy as np
import json
import ast

import os
import requests
import shutil


def fetch_json(update=False):
    """
    download the raw data from TrackMaven

    :return:
    :rtype:
    """
    print("Fetching data from source...")
    if os.path.exists("raw_data/newdump.json") and not update:  # don't download if path exists, and not forcing update
        print("Raw data already exists.")
    else:
        print("Downloading json...")
        req = requests.get("https://s3.amazonaws.com/temp-data-pulls/newdump.json", stream=True)
        with open("raw_data/newdump.json", 'wb') as file:
            shutil.copyfileobj(req.raw, file)
    print("Download complete.")


def get_platform_data(platform_name, update=False):
    """
    platform_name = 'pin' for pinterest
    platform_name = 'blog post'

    :param platform_name:
    :type platform_name:
    :return:
    :rtype:
    """
    assert type(platform_name) is str, "platform_data() requires str, passed type {}".format(type(platform_name.replace(' ', '_')))
    print("Getting data for {}...".format(platform_name))
    if os.path.exists("data/{}_data.csv".format(platform_name)) and not update:
        df_platform = pd.read_csv("data/{}_data.csv".format(platform_name.replace(' ', '_')))
    else:
        df = pd.read_json("raw_data/newdump.json")
        df_platform = df[df['type'] == platform_name]

        df_platform.to_csv("data/{}_data.csv".format(platform_name.replace(' ', '_')), index=False)

    print("{} data loaded.".format(platform_name))

    return df_platform


def clean_data(df):
    """
    clean the data

    :param df:
    :type df:
    :return:
    :rtype:
    """
    assert isinstance(df, pd.DataFrame), "clean_data() requires pd.Dataframe, passed type {}".format(type(df))

    # df.drop(["has_spend"], axis=1, inplace=True)  # TODO (@messiest) why are we dropping this?

    channel_info = df['channel_info'].apply(pd.Series)  # separate the columns of the dataframe  .apply(ast.literal_eval)
    content_info = df['content'].apply(pd.Series)  # separating out the columns the content_info column  .apply(ast.literal_eval)

    channel_info.columns = ["channel", "info"]  # set column names

    for x in content_info.columns:  # replace null values
        if "count" in x:
            content_info[x].fillna(np.NaN, inplace=True)

    df_new = pd.concat([df, channel_info, content_info], axis=1)
    df_new.drop(['channel_info', 'content'], axis=1, inplace=True)

    return df_new


def main(platform='blog post'):
    fetch_json()
    df = get_platform_data(platform, update=True)
    df_new = clean_data(df)

    return df_new


if __name__ == "__main__":
    print("Running pre-processing...")
    main()
