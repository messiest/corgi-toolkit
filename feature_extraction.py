import re
import sys

import gensim
import numpy as np
import pandas as pd
from gensim import corpora
from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer
from stop_words import get_stop_words
from textblob import TextBlob
import string

from rekognition import rekognition
from toolbox import blog_tools, pinterest_tools  # add additional toolkits here


def lemmatizing(df, series, stop_words=True):
    """
    when a word is lemmatized, contractions are rightfully turned into different stems since 's = is
    however, in reality, all of those words are themselves stop words, so I want to exclude them
    question marks and the like are not helpful for our purpose of figuring out potential categories


    :param df: dataframe to be lemmatized
    :type df:
    :param series:
    :type series:
    :param stop_words:
    :type stop_words:
    :return:
    :rtype:
    """
    lemmatizer = WordNetLemmatizer()
    en_stop = get_stop_words('en')

    contractions = [i for i in open('lists/contractions.txt')]

    texts = []  # list for tokenized documents in loop

    post_text = [i for i in df[series]]  # loop through document list

    print("Initializing tokenizer and lemmatizer ...")
    print("  Progress:")
    for i, text in enumerate(post_text):
        if not i % 1000:
            print("\t{}/{}".format(i, len(post_text)))

        tokens = word_tokenize(text.lower())  # clean and tokenize document string

        if stop_words:  # stem tokens and remove stop words
            lemmed_tokens = [lemmatizer.lemmatize(j) for j in tokens if not j in en_stop]
        else:
            lemmed_tokens = [lemmatizer.lemmatize(j) for j in tokens]

        contracted_tokens = [j for j in lemmed_tokens if not j in contractions]  # remove stemmed contractions

        # add tokens to list
        texts.append(contracted_tokens)

    print("Lemmatizing Completed.")

    return texts


def remove_stop_words(word_list, language='en'):
    """
    remove stop words from list of words

    :param word_list: list of words to remove stops
    :type word_list: list
    :param language: language for stop words
    :type language: list
    :return: list of words without stop words
    :rtype: list
    """

    word_list = [str(word) for word in word_list]  # coerce all members to type str

    print('Removing stop words...')
    en_stop = get_stop_words(language)
    no_stop_words = [i for i in word_list if not i in en_stop]
    print('Stop words removed.')

    return no_stop_words


def get_title_features(df, series, lem_list, word_list=['best', 'sex', 'now', 'new', 'episode', 'how']):
    """
    get fea

    :param df:
    :type df:
    :param series:
    :type series:
    :param lem_list:
    :type lem_list:
    :param word_list:
    :type word_list:
    :return:
    :rtype:
    """

    print('Initializing title feature extraction...')

    stopped_titles = remove_stop_words(lem_list)  # tokenize and lemmatize to count the length
    df['title_length'] = [len(stopped_titles[i]) for i in range(len(stopped_titles))]  # adding to dataframe
    celebs = {i.replace("'", "").strip('\n') for i in open('lists/celebs.txt')}  # get set from file (remove duplicates)

    # feature extraction for titles
    df['title_is_question'] = ['?' in i for i in df[series]]
    df['title_contains_number'] = [any(x in i for x in string.digits) for i in df[series]]
    df['title_contains_celeb'] = [any(x in i for x in celebs) for i in df[series]]

    for word in word_list:
        # print('Creating column for {}'.format(word))
        df["title_contains_{}".format(word)] = [word in i.lower() for i in df[series]]

    print('Title feature extraction complete.')
    full_df = df

    return full_df


def topic_modeling(df, lem_list, n_topics=5, n_words=30, n_passes=3):
    """

    :param df:
    :type df:
    :param lem_list:
    :type lem_list:
    :param number_of_topics:
    :type number_of_topics:
    :param number_of_words:
    :type number_of_words:
    :param number_of_passes:
    :type number_of_passes:
    :return:
    :rtype:
    """
    print("Initializing Topic Modeling...")

    dictionary = corpora.Dictionary(lem_list)  # turn our tokenized documents into a id:term dictionary
    corpus = [dictionary.doc2bow(text) for text in lem_list]  # convert tokenized documents into a document-term matrix

    print("Generating Model...")
    ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=n_topics, id2word=dictionary, passes=n_passes)
    topics = ldamodel.print_topics(num_topics=n_topics, num_words=n_words)
    print("Topics\n")

    for i in range(n_topics):
        print(f"  topic {topics[i][0]}: \n")
        print(topics[i][1], "\n")

    topic_vector = ldamodel[corpus]  # retrieve ldamodel[corpus] # retrieve topic_vector

    print("TOPIC VECTOR: ", topic_vector)

    print("Adding topic probabilities to DataFrame...")

    for j in range(n_topics):
        print('  Adding topic {}...'.format(j))
        df["topic_{}".format(j)] = [topic_vector[i][j][1] if len(topic_vector[i]) == n_topics else np.NaN for i in range(len(topic_vector))]

    print("Percetange of observations missing topic values: {}%".format(df['topic_0'].isnull().sum()/df.shape[0]*100))

    return df


def clean_text(text):
    """
    remove urls and low frequency words in text

    :param text: the body of text
    :type text: str
    :return: cleaned body of text
    :rtype: str
    """
    text = str(text)  # cast to string
    text = re.sub(r"http\S+", "", text)  # remove urls from text
    text = re.sub("\S{13,}", "", text)  # remove words over 13 characters

    return text


def get_polarity(text):
    """
    return polarity score for given text

    :param text: body of text to be evaluated
    :type text: str
    :return: polarity
    :rtype: float
    """
    cleaned = clean_text(text)
    polarity = TextBlob(cleaned).polarity

    return polarity


def get_subjectivity(text):
    """
    return subjectivity score for given text

    :param text: body of text to be evaluated
    :type text: str
    :return: polarity
    :rtype: float
    """
    cleaned = clean_text(text)
    subjectivity = TextBlob(cleaned).subjectivity

    return subjectivity


def image_objects(df):  # TODO (@messiest) Get this from / move this to the rekognition/rekognition_results.py
    """
    get arrays for objects detected in images

    :param df:
    :type df:
    :return:
    :rtype:
    """
    # images = rekognition.run(df['id'])
    images = rekognition.run(df['uniqueid'])
    image_detects = {}
    for img in images.keys():
        objects = images[img]['objects']
        try:
            image_detects[img] = objects['Labels']
        except:
            image_detects[img] = objects

    objects = {}
    for j in image_detects.values():
        if isinstance(j, dict):
            for k in j.keys():
                if k not in objects.keys():
                    objects[k] = []


    for k in image_detects.keys():
        objs = image_detects[k]
        if isinstance(objs, dict):
            for o in objects:
                if o in objs.keys():
                    objects[o].append(objs[o])
                else:
                    objects[o].append(0)


    image_df = pd.DataFrame(objects)

    return image_df


def main(medium_type, publication, feature_words=None):

    word_list = [word.replace('\n', '') for word in open('lists/top_words.txt')]

    if feature_words:
        word_list = [i.lower() for i in feature_words]

    lemmatized_titles = None
    df = None
    if medium_type == 'blog post':
        df = blog_tools.main(publication)

        lemmatized_titles = lemmatizing(df, 'title', stop_words=False)

        df = get_title_features(df, 'title', lemmatized_titles, word_list)

        df['description_polarity'] = df['blog_post'].apply(get_polarity)  # get polarity scores
        df['description_subjectivity'] = df['blog_post'].apply(get_subjectivity)  # get subjectivity scores

        df['title_polarity'] = df['title'].apply(get_polarity)  # get polarity scores
        df['title_subjectivity'] = df['title'].apply(get_subjectivity)  # get subjectivity scores

        images = image_objects(df)  # load images

        new_titles = lemmatizing(df, 'title', stop_words=True)

        df = topic_modeling(df, new_titles)

        df.reset_index(drop=True)
        images.reset_index(drop=True)

        print("DF: ", df.shape)
        print("IMAGES:", images.shape)

        df = df.merge(images, left_index=True, right_index=True)

        print(df.shape)

    elif medium_type == 'pin':
        df = pinterest_tools.main()

        lemmatized_titles = lemmatizing(df, 'description', stop_words=False)
        df = get_title_features(df, 'description', lemmatized_titles, word_list)

        df['title_polarity'] = df['title'].apply(get_polarity)
        df['title_subjectivity'] = df['title'].apply(get_subjectivity)

        df = topic_modeling(df, lem_list=lemmatized_titles)

        images = image_objects(df)  # load image detection
        new_titles = lemmatizing(df, 'title', stop_words=True)

        df = topic_modeling(df, lem_list=new_titles)

        df.reset_index(drop=True)
        images.reset_index(drop=True)

        print(df.shape)
        print(images.shape)

        df = df.merge(images, left_index=True, right_index=True)

        print(df.shape)

    df.to_csv('processed_data/{}_{}.csv'.format(publication, medium_type.replace(' ', '_')))

    return df


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2])

    except IndexError:
        platform = input('What platform would you like to analyze? ')
        print("Analyzing {}...".format(platform))

        publication = None
        if platform == 'blog post':
            publication = input('Which publication would you like to analyze? ')

        main(platform, publication)
