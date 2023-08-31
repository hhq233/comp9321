import pandas as pd
from pandas.api.types import is_numeric_dtype
from sklearn import linear_model
from sklearn.utils import shuffle
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import OrdinalEncoder
import sys
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression, chi2
import scipy.stats
import csv
from sklearn.metrics import confusion_matrix
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import precision_score, accuracy_score, recall_score


train_data = pd.read_csv(sys.argv[1], sep='\t')
test_data = pd.read_csv(sys.argv[2], sep='\t')

def transform_df(df):
    columns = list(df.columns)
    for column in columns:
        if not is_numeric_dtype(df[column]):
            encGhrp = OrdinalEncoder()
            df[column] = encGhrp.fit_transform(df[[column]])
            df = df.fillna(0)
    return  df

def part_1(train_data, test_data):
    # if a feature include majority (90%) part is same data, means its change has less
    # to predict result
    total_item = len(train_data.index)
    for column in train_data.drop('revenue', axis=1).columns:
        d = train_data[column].value_counts().head(1)
        percent = d.values / total_item
        if percent > 0.9:
            train_data = train_data.drop(column, axis=1)
            test_data = test_data.drop(column, axis=1)

    train_data = shuffle(train_data)
    train_x = train_data.drop('revenue', axis=1).values
    train_y = train_data['revenue'].values

    test_x = test_data.drop('revenue', axis=1).values
    #test_y = test_data['revenue'].values

    model = linear_model.LinearRegression()
    model.fit(train_x, train_y)

    y_pred = model.predict(test_x)

    """print("Mean squared error: %.2f"
          % mean_squared_error(test_y, y_pred))

    print(scipy.stats.pearsonr(y_pred, test_y))"""

    with open('z5295162.PART1.output.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['predicted_revenue'])
        for i in y_pred:
            writer.writerow([i])

def part_2(train_data, test_data):
    total_item = len(train_data.index)
    for column in train_data.columns:
        d = train_data[column].value_counts().head(1)
        percent = d.values / total_item
        if percent > 0.9:
            train_data = train_data.drop(column, axis=1)
            test_data = test_data.drop(column, axis=1)

    train_data = shuffle(train_data)
    train_x = train_data.drop('rating', axis=1).values
    train_y = train_data['rating'].values

    select = SelectKBest(score_func=chi2, k=8)
    z = select.fit_transform(train_x, train_y)

    filter = select.get_support()
    drop_list = []
    for i in range(0, len(filter)):
        if filter[i] == False:
            f = list(test_data.columns)[i]
            drop_list.append(f)
    for feature in drop_list:
        test_data = test_data.drop(feature, axis=1)

    test_x = test_data.drop('rating', axis=1).values
    #test_y = test_data['rating'].values

    # train a classifier
    knn = KNeighborsClassifier(p=1, n_neighbors=20)
    knn.fit(train_x, train_y)
    # predict the test set
    predictions = knn.predict(test_x)

    """print("confusion_matrix:\n", confusion_matrix(test_y, predictions))
    print("precision:\t", precision_score(test_y, predictions, average=None))
    print("recall:\t\t", recall_score(test_y, predictions, average=None))
    print("accuracy:\t", accuracy_score(test_y, predictions))"""

    with open('z5295162.PART2.output.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['predicted_rating'])
        for i in predictions:
            writer.writerow([i])


if __name__ == "__main__":
    train_data = transform_df(train_data)
    test_data = transform_df(test_data)
    part_1(train_data.drop(['Average_Wait_Time','Day_Type','rating'], axis=1), test_data.drop(['Average_Wait_Time','Day_Type','rating'], axis=1))
    part_2(train_data.drop(['revenue'], axis=1), test_data.drop(['revenue'], axis=1))