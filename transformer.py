import pandas as pd

CSV_PATH = '/home/duy.caov/work_space/apps/PLOTLY_SANKEY/input_data.csv'

df = pd.read_csv(CSV_PATH)

# Fill na
df = df.fillna("Unidentified")

# Rename 
df.replace(to_replace='1-Active Customer', value='1-Active', inplace=True)

df.to_csv('/home/duy.caov/work_space/apps/PLOTLY_SANKEY/transformed_data.csv', index = 0)
