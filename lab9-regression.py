import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error
from sklearn.utils import shuffle


df = pd.read_csv('diet.csv', index_col=0)

diet_x = df.drop('weight6weeks', axis=1).values
diet_y = df['weight6weeks'].values

print(diet_x)