import pandas as pd

CSV_PATH = '/home/duy.caov/work_space/apps/PLOTLY_SANKEY/input_data.csv'

df = pd.read_csv(CSV_PATH)

df = df.fillna("Unidentified")

df.to_csv('/home/duy.caov/work_space/apps/PLOTLY_SANKEY/transformed_data.csv', index = 0)
