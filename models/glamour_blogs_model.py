import os
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report
from sklearn.externals import joblib

import feature_extraction

MODEL_FILE = '../jars/glamour_blogs.pkl'
DATA_FILE = '../data/glamour_blogs.csv'

def main():
    if os.path.exists(MODEL_FILE):
        logistic = joblib.load(MODEL_FILE)
    else:
        logistic = LogisticRegression()

    if not os.path.exists(DATA_FILE):
        feature_extraction.main('blog post', 'glamour')

    df = pd.read_csv(DATA_FILE)

    print(df.columns)

    quit()

    # TODO (@messiest) y ~ 1 if y in top 25%, 0 else

    y = df.pop('impact')
    x = None

    x_train, x_test, y_train, y_test = train_test_split(x, y)
    logistic.fit(x_train, x_test)
    predictions = logistic.predict(x_test)
    print(classification_report(y_test, predictions))

    joblib.dump(logistic, MODEL_FILE)


if __name__ == "__main__":
    main()