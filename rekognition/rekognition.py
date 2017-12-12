"""
Used with creator's permission.

From: https://github.com/messiest/amzn-rekognition
"""
import pickle
import sys
import time

import boto3
import pandas as pd

from s3.s3_access import S3Bucket

client = boto3.client('rekognition')


def object_detection(bucket, image, threshold=75):
    """
    run object detection on image

    :param bucket: name of s3 bucket
    :type bucket: str
    :param image: file name of image
    :type image: str
    :param threshhold: starting confidence threshold
    :type threshhold: int
    :return: dictionary of labels
    :rtype: dict
    """
    global client

    response = client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': image}}, MinConfidence=threshold)

    if printer:
        print('Detected labels in ' + image)
        print(list(response.keys()))

    labels = {i['Name']: i['Confidence'] for i in response['Labels']}  # dictionary of features:confidences
    return labels


def text_detection(bucket, image):
    """
    run text detection on image

    :param bucket: name of s3 bucket
    :type bucket: str
    :param image: file name of image
    :type image: str
    :param threshhold: starting confidence threshold
    :type threshhold: int
    :return: dictionary of labels
    :rtype:
    """
    global client

    response = client.detect_text(Image={'S3Object': {'Bucket': bucket, 'Name': image}})
    if printer:
        print('Detected celebrities in ' + image)
        print(list(response.keys()))

    labels = []
    if response['TextDetections']:
        labels = [i['DetectedText'] for i in response['TextDetections']]

    return labels


def face_detection(bucket, image):
    """
    run text detection on image

    :param bucket: name of s3 bucket
    :type bucket: str
    :param image: file name of image
    :type image: str
    :param threshhold: starting confidence threshold
    :type threshhold: int
    :return: dictionary of labels
    :rtype:
    """
    global client

    response = client.detect_faces(Image={'S3Object': {'Bucket': bucket, 'Name': image}})
    # if printer:
    #     print('Detected celebrities in ' + image)
    #     print(list(response.keys()))

    # labels = []
    # if response['TextDetections']:
    #     labels = [i['DetectedText'] for i in response['TextDetections']]

    return response


def moderation_detection(bucket, image):
    """
    run moderation detection on image

    :param bucket: name of s3 bucket
    :type bucket: str
    :param image: file name of image
    :type image: str
    :param threshhold: starting confidence threshold
    :type threshhold: int
    :return: dictionary of labels
    :rtype:
    """
    global client

    response = client.detect_moderation_labels(Image={'S3Object': {'Bucket': bucket, 'Name': image}})
    if printer:
        print('Moderation labes in ' + image)
        print(list(response.keys()))

    labels = []
    if response['ModerationLabels']:
        labels = {i['Name']: i['Confidence'] for i in response['ModerationLabels']}  # dictionary of features:confidences

    return labels


def run(images):
    """
        Run rekognition for pipeline.

        :param n: number of images to run
        :type n: int
        :return: results of running amazon's rekognition
        :rtype: dict
    """
    global client

    print("Running image processing...")
    # images = ["{}.jpg".format(i) for i in images]
    results = {}

    images = [img + ".jpg" for img in images]

    try:
        existing = pickle.load(open('jar/image-tags.pkl', 'rb'))
        print('Pickle exists...\n', 'Pickle loaded.')
        print("  Total Images: {}".format(len(existing.keys())))

    except:
        print('No Pickle found.\nCreating new data object.')
        existing = {}

    for i, image in enumerate(images):
        start = time.time()
        print("{} - {}".format(i, image))
        try:
            if image in existing.keys():  # skip files that have already been processed
                results[image] = existing[image]
                continue
            else:
                results[image] = {}
                results[image]['objects'] = object_detection('trackmaven-images', image)
                results[image]['moderation'] = moderation_detection('trackmaven-images', image)
                results[image]['text'] = text_detection('trackmaven-images', image)
                results[image]['faces'] = face_detection('trackmaven-images', image)

                existing[image] = results[image]

        except:
            print("Error: ", image)
            results[image] = {i: None for i in ['objects', 'moderation', 'text', 'faces'] if
                              i not in results[image].keys()}

        print(" timer: {:2f}".format(time.time() - start))

    pickle.dump(existing, open('jar/image-tags.pkl', 'wb'))

    return results

def main(n=10):
    """
    Run image recognition

    :param n: number of images to run
    :type n: int
    :return: results of running amazon's rekognition
    :rtype: dict
    """
    global bucket

    print("Running image processing...")
    try:
        results = pickle.load(open('jar/image-tags.pkl', 'rb'))
        print('loaded pickle')
        print("Total Images: {}".format(len(results.keys())))

    except:
        results = {}

    processed_images = 0
    while processed_images < n:
        for i, image in enumerate(bucket.sample(n)):
            print(i, image)
            try:
                if image in results.keys():  # skip files that have already been processed
                    continue
                else:
                    results[image] = {}

                results[image]['objects'] = object_detection('trackmaven-images', image)
                results[image]['moderation'] = moderation_detection('trackmaven-images', image)
                results[image]['text'] = text_detection('trackmaven-images', image)
                results[image]['faces'] = face_detection('trackmaven-images', image)

            except:
                print("Error: ", image)
                results[image] = {i: None for i in ['objects', 'moderation', 'text', 'faces'] if i not in results[image].keys()}
        break

    pickle.dump(results, open('jar/image-tags-copy.pkl', 'wb'))

    return results


if __name__ == "__main__":
    printer = False

    service = 'rekognition'
    print("Connecting to {}...".format(service))
    client = boto3.client(service)
    bucket = S3Bucket('trackmaven-images')
    bucket.connect()

    try:
        # features = main(n=int(sys.argv[1]))
        df = pd.read_csv(sys.argv[1], index_col=0)
        images = list(list(df['uniqueid'] + ".jpg"))
        features = run(images)

    except IndexError:
        print("ERROR")

    for i in features.keys():
        print("Image: ", i)
        print('Objects: ', features[i]['objects'])
        print('Text: ', features[i]['text'])
        print('Moderation: ', features[i]['moderation'])
        print('Faces: ', features[i]['faces'])
