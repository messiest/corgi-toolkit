import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import sys
import os
import requests
import shutil

import process_raw


def scraper_links(url, publication):

    article = ""
    domain = publication
    print("Fetching {}".format(url))

    ## TRY TO SEE IF THE LINK EXISTS. ELSE JUST RETURN NONE AND MOVE ON.
    try:
        request = requests.get(url)
        soup = BeautifulSoup(request.text, "lxml")
        info = json.loads(soup.find('script', type='application/ld+json').text)
    except:
        article = "Link is Dead"
        other_info = {'headline': None, 'image': None, 'description': None, 'keywords': None, '@type': None}
        return article, other_info

    ## IF LINK WORKS, USE JSON ELSE USE BEAUTIFUL SOUP TO GET DATA.
    try:
        article = info["articleBody"]

    except:
        try:
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
    print("Creating DataFrame from blogs")
    blogs = df[df['type'] == 'blog post']  # create new df called blogs that only contains blogs
    blogs.reset_index(inplace=True)

    blogs['url'] = blogs['link'].astype(str)  # converts link to string so we can split

    new_mag = []  # instantiate a new list called new_mag

    # list comprehension that just keeps part before '.com'
    # we can use list comprehension because this is true for all values
    magazine = [i.split('.com')[0] for i in blogs['url']]
    # start for loop to get rid of everything before the name of the magazine

    for i in magazine:
        if '.' in i:
            new_mag.append(i.split('.')[1])  # if there isn't a '.' it just sends the existing name to the list
        else:
            new_mag.append(i)
    blogs['pub'] = new_mag  # create new column for the blog df with the publications

    return blogs


def get_publication(df, publication):
    print("Getting {} data...".format(publication))
    pubs = df[df['pub'] == publication]

    pubs.to_csv('publication_data/{}.csv'.format(publication))

    return pubs


def main(publication="allure"):
    if not os.path.exists('publication_data/{}.csv'.format(publication)):
        df_new = process_raw.main()
        df_blogs = create_blogs_df(df_new)
        pub = get_publication(df_blogs, publication)

    else:
        pub = pd.read_csv('publication_data/{}.csv'.format(publication), index_col=0)

    pub = pub.head(5)

    pub['scraped'] = pub['link'].apply(lambda x: scraper_links(x, publication))

    pub = pd.concat([pub, pub['scraped'].apply(pd.Series)], axis=1)
    pub.columns.values[-2] = 'blog_post'
    pub.columns.values[-1] = 'blog_details'
    pub.drop('scraped', axis=1, inplace=True)

    print(pub)

    return pub


if __name__ == "__main__":
    try:
        main(sys.argv[1])

    except IndexError:
        main()
