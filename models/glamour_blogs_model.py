import os
import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report
from sklearn.externals import joblib

import feature_extraction

MODEL_FILE = '../jars/glamour_blogs.pkl'
DATA_FILE = '../processed_data/glamour_blog post.csv'

def main():
    if os.path.exists(MODEL_FILE):
        logistic = joblib.load(MODEL_FILE)
    else:
        logistic = LogisticRegression()

    if not os.path.exists(DATA_FILE):
        feature_extraction.main('blog post', 'glamour')

    df = pd.read_csv(DATA_FILE, index_col=0)

    # TODO (@messiest) y ~ 1 if y in top 25%, 0 else

    y = df['engagement'] #   .apply(lambda x: 0 if x == np.NaN else x)
    print(df.shape, df.iloc[0, 0:5], y.isnull().sum())

    x = df.iloc[:, 28:]

    quit()

    x_train, x_test, y_train, y_test = train_test_split(x, y)
    logistic.fit(x_train, x_test)
    predictions = logistic.predict(x_test)
    print(classification_report(y_test, predictions))

    joblib.dump(logistic, MODEL_FILE)


if __name__ == "__main__":
    main()