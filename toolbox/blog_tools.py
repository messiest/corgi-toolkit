import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import sys
import os
import requests
import shutil

import process_raw


def scrape_blog(url, publication):
    """
    get text of blog post


    :param url: url to the blog post
    :type url: str
    :param publication: the publication's name
    :type publication: str
    :return: (article, info)
    :rtype: tuple
    """
    article = ""
    domain = publication
    print("Fetching: {}".format(url))

    try:  # verify if the link exists
        request = requests.get(url)
        soup = BeautifulSoup(request.text, "lxml")
        info = json.loads(soup.find('script', type='application/ld+json').text)
    except:
        article = "Link is Dead"
        other_info = {'headline': None, 'image': None, 'description': None, 'keywords': None, '@type': None}
        return article, other_info

    try:  # use json if present, otherwise use beautiful soup
        article = info["articleBody"]

    except:
        try:  # TODO (@messiest) These should be moved to separate functions
            if domain == "glamour":
                for posts in soup.find_all('div', {'class': 'article__content'}):
                    article = posts.text

            elif domain == "teenvogue":
                for posts in soup.find_all('div', {'class': 'content article-content'}):
                    article = posts.text

            elif domain == "wmagazine":
                for posts in soup.find_all('div', {'class': 'body-text'}):
                    article = posts.text

            elif domain == "allure":
                for posts in soup.find_all('div', {'class': 'article-body'}):
                    article = posts.text

            elif domain == "cntraveler":
                for posts in soup.find_all('div', {'class': 'article-body'}):
                    article = posts.text

            elif domain == "architecturaldigest":
                for posts in soup.find_all('div', {'class': 'article-content-main'}):
                    article = posts.text

            elif domain == "vogue":
                for posts in soup.find_all('div', {'class': 'article-copy--body'}):
                    article = posts.text
        except:
            article = "Article Not Found"

    try:  # use json to extract image info and other stuff else return none
        attributes = {'headline', 'image', 'description', 'keywords', '@type'}
        other_info = {k: v for k, v in info.items() if k in attributes}
    except:
        other_info = {'headline': None, 'image': None, 'description': None, 'keywords': None, '@type': None}

    return article, other_info


def create_blogs_df(df):
    """
    return subset of the dataframe that is just blog posts

    :param df: data
    :type df: pd.DataFrame
    :return: b
    :rtype:
    """
    print("Creating DataFrame of blog posts...")

    blogs_df = df[df['type'] == 'blog post']  # create new df called blogs_df that only contains blogs
    blogs_df.reset_index(inplace=True)
    blogs_df['url'] = blogs_df['link'].astype(str)  # converts link to string so we can split

    new_mag = []  # instantiate a new list called new_mag
    magazine = [i.split('.com')[0] for i in blogs_df['url']]  # remove all but site name

    for i in magazine:  # start for loop to get rid of everything before the name of the magazine
        if '.' in i:
            new_mag.append(i.split('.')[1])  # if there isn't a '.' it just sends the existing name to the list
        else:
            new_mag.append(i)

    blogs_df['pub'] = new_mag  # create new column for the blog df with the publications

    return blogs_df


def get_publication(df, publication):
    """
    return subset of the dataframe for publication

    :param df:
    :type df:
    :param publication:
    :type publication:
    :return:
    :rtype:
    """
    print("Getting {} data...".format(publication))
    pubs = df[df['pub'] == publication]

    pubs.to_csv('publication_data/{}.csv'.format(publication))

    return pubs


def main(publication="vogue"):
    if not os.path.exists('publication_data/{}.csv'.format(publication)):
        df_new = process_raw.main()
        df_blogs = create_blogs_df(df_new)
        pub = get_publication(df_blogs, publication)

    else:
        pub = pd.read_csv('publication_data/{}.csv'.format(publication), index_col=0)

    pub['scraped'] = pub['link'].apply(lambda x: scrape_blog(x, publication))  # scrape blog post

    pub = pd.concat([pub, pub['scraped'].apply(pd.Series)], axis=1)
    pub.columns.values[-2] = 'blog_post'
    pub.columns.values[-1] = 'blog_details'
    pub.drop('scraped', axis=1, inplace=True)

    return pub


if __name__ == "__main__":
    try:
        main(sys.argv[1])

    except IndexError:
        main()
