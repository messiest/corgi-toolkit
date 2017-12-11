import pickle
from sklearn.feature_extraction.text import CountVectorizer


def get_text(file_name):
    file = open(file_name, 'rb')
    data = pickle.load(file)

    words = []
    for image in data.keys():
        if data[image]['text']:
            for word in data[image]['text']:
                words.append(word.lower())

    print("{} words, {} unique".format(len(words), len(set(words))))

    return words


def get_objects(file_name):
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


def main():
    file = 'jar/image-tags.pkl'

    print(get_moderation(file))
    print(get_text(file))
    print(get_objects(file))


if __name__ == "__main__":
    main()
