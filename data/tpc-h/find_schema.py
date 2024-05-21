import json
import pandas as pd
def export_schema(df):
    # Define a mapping of Pandas dtypes to the desired output types
    dtype_mapping = {
        'int64': 'int',
        'float64': 'float',
        'object': 'str',
        'datetime64[ns]': 'date'
    }
    
    # Initialize the schema dictionary
    schema = {}
    
    # Iterate over the DataFrame columns and their dtypes
    for col, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        if dtype_str in dtype_mapping:
            schema[col] = dtype_mapping[dtype_str]
        else:
            schema[col] = 'str'  # Default to 'str' if dtype is not recognized
    
    # Convert the schema dictionary to the desired format
    schema_output = {
        "schema": schema
    }
    
    return schema_output

df = pd.read_parquet('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/sf10_2/denormalized.parquet/0.parquet')
schema = export_schema(df)
with open('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/workload2.json', 'r') as f:
    inp = json.load(f)

schema['schema'] = {k.lower(): v for k, v in schema['schema'].items() }
inp['schema'] = schema['schema']
with open('/Users/umsaka/Documents/DDCPS/qdtree/data/tpc-h/workload3.json', 'w') as f:
    json.dump(inp, f, indent=4)