import pickle
from sklearn.feature_extraction.text import CountVectorizer


def get_text():
    file = open('jar/image-tags.pkl', 'rb')
    data = pickle.load(file)

    words = []
    for image in data.keys():
        if data[image]['text']:
            for word in data[image]['text']:
                words.append(word.lower())

    print("{} words, {} unique".format(len(words), len(set(words))))

    return words


def get_objects():
    file = open('jar/image-tags.pkl', 'rb')
    data = pickle.load(file)

    objects = []
    for image in data.keys():
        if data[image]['objects']:
            for word in data[image]['objects']:
                objects.append(word.lower())

    print("{} objects, {} unique".format(len(objects), len(set(objects))))

    return objects


def get_moderation():
    file = open('jar/image-tags.pkl', 'rb')
    data = pickle.load(file)

    moderation = []
    for image in data.keys():
        if data[image]['moderation']:
            for word in data[image]['moderation']:
                print(word)
                moderation.append(word.lower())

    print("{} moderation, {} unique".format(len(moderation), len(set(moderation))))

    return moderation


def main():
    get_moderation()
    # get_text()
    # get_objects()


if __name__ == "__main__":
    main()
