import os
import pandas as pd
import re
from concurrent.futures import ProcessPoolExecutor

data_dir = ''
num_cores = 4  # Can change this

year_strings = [str(year) for year in range(1968, 2011)]

all_files = [
    os.path.join(data_dir, f)
    for f in os.listdir(data_dir)
    if os.path.isfile(os.path.join(data_dir, f))
    and f.lower().endswith('.csv')
]

def read_and_annotate(file):
    df = pd.read_csv(file)
    filename = os.path.basename(file)

df_list = []

if all_files:
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        for df in executor.map(read_and_annotate, all_files):
            df_list.append(df)

if df_list:
    full_df = pd.concat(df_list, ignore_index=True)
else:
    full_df = pd.DataFrame()  # Empty DataFrame if no files found

# Keep only the required columns before saving
columns_to_keep = ['datayear', 'cntyrfip', 'stateres', 'cntyres', 'ocntyfips']
full_df = full_df.loc[:, [col for col in columns_to_keep if col in full_df.columns]]

output_path = ''
full_df.to_parquet(output_path, index=False)
