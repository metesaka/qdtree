import os
import pandas as pd
import tqdm

files = os.listdir('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/sf10/denormalized.parquet')
for file in tqdm.tqdm(files):
    if file.endswith('.parquet'):
        sample = pd.read_parquet('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/sf10/denormalized.parquet/'+file).sample(frac=0.1)
        sample.to_parquet('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/sf10_2/denormalized.parquet/'+file)