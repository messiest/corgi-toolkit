import sys
import pandas as pd
import numpy as np
import requests

import boto3

from s3_access import S3Bucket

BUCKET_NAME = "trackmaven-images"

s3 = boto3.resource('s3')


def download_image(bucket, key, url):
    global s3
    try:
        print("  Downloading: ", url)
        image = requests.get(url, stream=True, timeout=3)                                   # stream image from url
        s3.Bucket(bucket).put_object(Key="{}.jpg".format(key), Body=image.raw.read())       # upload to s3

        return True

    except requests.exceptions.ReadTimeout:
        return False


def update_image_bucket(bucket_name, image_file=None, batch_size=None):
    """
    update local files with images that have not yet been transferred to the s3 bucket

    :param bucket_name: S3 bucket to access
    :type bucket_name: str
    :param batch_size: number of images to download
    :type batch_size: int
    :return None
    """
    bucket = S3Bucket(bucket_name)                         # instantiate bucket
    bucket.connect()                                       # connect to bucket
    images = bucket.get_keys()                             # get list of file names

    data = pd.read_csv(image_file, low_memory=False)

    print("Loading image data...")
    if batch_size is None:
        batch_size = data.shape[0]                         # total posts

    print("Fetching {} images...".format(batch_size))

    downloaded_images = 0                                  # counter for downloads
    for i, row in data.iterrows():                         # iterate over rows in chunk
        key = row['uniqueid']                              # get image key
        url = row['image_url']                             # get image url
        file_name = "{}.jpg".format(key)                   # generate file name

        if file_name not in images:                        # skip if file exists in bucket
            if url is not np.NaN:                          # skip if NaN
                print("{}/{} - ".format((i + 1), batch_size, i), key)
                image_downloaded = download_image(bucket_name, key, url)
                if image_downloaded:                       # increment if download was successful
                    downloaded_images += 1

        if downloaded_images == batch_size:  # end if batch is completed
            print("Batch complete.")
            return


def main(bucket_name=BUCKET_NAME):
    print("Running image downloader...")
    print("Connected to S3? ", bool(s3))
    image_file = 'data/blog_images.csv'
    batch_size = None
    try:
        image_file = sys.argv[1]
        batch_size = int(sys.argv[2])
        update_image_bucket(bucket_name,
                            image_file=image_file,
                            batch_size=batch_size)
    except IndexError:
        update_image_bucket(bucket_name,
                            image_file=image_file,
                            batch_size=batch_size)


if __name__ == "__main__":
    main()
