import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
train_data = pd.read_csv('train.tsv', sep='\t')

df = train_data
""".groupby("Number_of_Shops_Around_ATM")["rating"].mean()
df = pd.DataFrame(df)
df.reset_index(inplace=True)
df.plot.scatter(x='Number_of_Shops_Around_ATM', y='rating', title='Number_of_Shops_Around_ATM')
plt.show()"""
sns.set_style('whitegrid')
sns.lmplot(x ='rating', y ='revenue', data = df, hue='ATM_Attached_to').set(title='ATM_Attached_to')
plt.show()