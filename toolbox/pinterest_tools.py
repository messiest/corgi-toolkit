import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import sys
import os
import requests
import shutil

import process_raw


def create_pinterest_df(df):
    """
    return subset of the dataframe that is just blog posts

    :param df: data
    :type df: pd.DataFrame
    :return: b
    :rtype:
    """
    print("Creating DataFrame of blog posts...")

    blogs_df = df[df['type'] == 'pin']  # create new df called blogs_df that only contains blogs
    blogs_df.reset_index(inplace=True)
    blogs_df['url'] = blogs_df['link'].astype(str)  # converts link to string so we can split

    return blogs_df


def main():
    """


    :param publication:
    :type publication:
    :return:
    :rtype:
    """
    # if not os.path.exists('../data/pinterest_data.csv'):
    #     df_new = process_raw.main()
    #
    # else:
    df_new = pd.read_csv('data/pin_images.csv', index_col=0)  # work around for posts

    return df_new


if __name__ == "__main__":
    try:
        main(sys.argv[1])

    except IndexError:
        print(main())
