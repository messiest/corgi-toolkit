import pickle
from sklearn.feature_extraction.text import CountVectorizer


"""
This file is used to return results from the dictionary that is created as a result of rekognition api calls.

"""

def get_text(file_path):
    """
    return the results of the file detection for the passed file_name

    :param file_path: file name of image file on
    :type file_path:
    :return:
    :rtype:
    """

    file = open(file_path, 'rb')
    data = pickle.load(file)

    words = []
    for image in data.keys():
        if data[image]['text']:
            for word in data[image]['text']:
                words.append(word.lower())

    print("{} words, {} unique".format(len(words), len(set(words))))

    return words


def get_objects(file_name):
    """
    return the results of the file detection for the passed file_name

    :param file_path: file name of image file on
    :type file_path:
    :return:
    :rtype:
    """

    file = open(file_name, 'rb')
    data = pickle.load(file)

    objects = []
    for image in data.keys():
        if data[image]['objects']:
            for word in data[image]['objects']:
                objects.append(word.lower())

    print("{} objects, {} unique".format(len(objects), len(set(objects))))

    return objects


def get_moderation(file_name):
    """
    return the results of the file detection for the passed file_name

    :param file_path: file name of image file on
    :type file_path:
    :return:
    :rtype:
    """
    file = open(file_name, 'rb')
    data = pickle.load(file)

    moderation = []
    for image in data.keys():
        if data[image]['moderation']:
            for word in data[image]['text']:
                print(word)
                moderation.append(word.lower())

    print("{} moderation, {} unique".format(len(moderation), len(set(moderation))))

    return moderation


# def run(files, pickle_='jar/image-tags.pkl'):
#     results = {}
#     for file in files:
#         pass
#     pass


def main():
    file = 'jar/image-tags.pkl'

    print(get_moderation(file))
    print(get_text(file))
    print(get_objects(file))


if __name__ == "__main__":
    main()
