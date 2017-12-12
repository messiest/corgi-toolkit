import os
import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report
from sklearn.externals import joblib

import feature_extraction

MODEL_FILE = '../jar/glamour_blogs.pkl'
DATA_FILE = '../processed_data/glamour_blog_post.csv'


def model():
    if os.path.exists(MODEL_FILE):
        logistic = joblib.load(MODEL_FILE)
    else:
        logistic = LogisticRegression(penalty='l2')

    if not os.path.exists(DATA_FILE):
        feature_extraction.main('blog post', 'glamour')

    df = pd.read_csv(DATA_FILE)

    df = df.fillna(0)

    y = df['impact'].apply(lambda x: 0 if x < df['impact'].mean() else 1)
    x = df.iloc[:, 36:]

    x_train, x_test, y_train, y_test = train_test_split(x, y)
    logistic.fit(x_train, y_train)
    predictions = logistic.predict(x_test)
    print(classification_report(y_test, predictions))

    joblib.dump(logistic, MODEL_FILE)

    features = dict(zip(x.columns, list(logistic.coef_[0])))

    return features


def get_best_features(features, n=10):
    results = [(k, features[k]) for k in sorted(features, key=features.get, reverse=True)]
    print("Best {} features:".format(n))
    output = {}
    for i, j in enumerate(results):
        k, v = j
        print("  ", k, v)
        output[k] = v
        if i == n:
            break

    return output


def get_worst_features(features, n=10):
    results = [(k, features[k]) for k in sorted(features, key=features.get, reverse=False)]
    print("Worst {} features:".format(n))
    output = {}
    for i, j in enumerate(results):
        k, v = j
        print("  ", k, v)
        output[k] = v
        if i == n:
            break

    return output


def main():
    results = model()
    best = get_best_features(results)
    worst = get_worst_features(results)

    print(best.keys())
    print(best.values())

    print(worst.keys())
    print(worst.values())

if __name__ == "__main__":
    main()
