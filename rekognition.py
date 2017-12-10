import sys
import pickle
import boto3
import botocore

from s3_access import S3Bucket


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
    :rtype:
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


def main(n=10):
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
            try:
                if image in results.keys():  # skip files that have already been processed
                    continue
                else:
                    results[image] = {}

                results[image]['objects'] = object_detection('trackmaven-images', image)
                results[image]['moderation'] = moderation_detection('trackmaven-images', image)
                results[image]['text'] = text_detection('trackmaven-images', image)

            except:
                print("Error: ", image)
                results[image] = {i: None for i in ['objects', 'moderation', 'text'] if i not in results[image].keys()}
        break

    pickle.dump(results, open('jar/image-tags.pkl', 'wb'))

    return results


if __name__ == "__main__":
    printer = False

    service = 'rekognition'
    print("Connecting to {}...".format(service))
    client = boto3.client(service)
    bucket = S3Bucket('trackmaven-images')
    bucket.connect()

    try:
        features = main(n=int(sys.argv[1]))
    except IndexError:
        features = main()

    for i in features.keys():
        print("Image: ", i)
        print('Objects: ', features[i]['objects'])
        print('Text: ', features[i]['text'])
        print('Moderation: ', features[i]['moderation'])
