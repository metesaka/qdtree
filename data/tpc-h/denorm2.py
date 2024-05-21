#!/usr/bin/env python3
#
# Based on: https://github.com/Jibbow/denormalized-tpch
#
# Example: ./denormalize.py ./sf1 --out ./sf1/denormalized.csv
#

import os
import argparse
import duckdb

# Parse command line arguments.
parser = argparse.ArgumentParser(description="Denormalize TCP-H tables.")
parser.add_argument(
    "tpch_dir",
    help="Directory containing the files generated by the TPC-H tool.",
)
parser.add_argument(
    "--out",
    default="denormalized.csv",
    metavar="out.csv",
    help="Specify a file where to write the denormalized file. Use .csv extension "
    "to write CSV file and .parquet extension to write Parquet file.",
)
parser.add_argument("--customer", default="customer.tbl", metavar="customer.tbl")
parser.add_argument("--lineitem", default="lineitem.tbl", metavar="lineitem.tbl")
parser.add_argument("--nation", default="nation.tbl", metavar="nation.tbl")
parser.add_argument("--orders", default="orders.tbl", metavar="orders.tbl")
parser.add_argument("--part", default="part.tbl", metavar="part.tbl")
parser.add_argument("--partsupp", default="partsupp.tbl", metavar="partsupp.tbl")
parser.add_argument("--region", default="region.tbl", metavar="region.tbl")
parser.add_argument("--supplier", default="supplier.tbl", metavar="supplier.tbl")
args = parser.parse_args()

# Check that all .tbl files generated by TPC-H tool are present.
tbl_files = os.listdir(args.tpch_dir)
required_files = [
    args.customer,
    args.lineitem,
    args.nation,
    args.orders,
    args.part,
    args.partsupp,
    args.region,
    args.supplier,
]
if not all(f in tbl_files for f in required_files):
    print(
        "The specified directory %s does not contain the required .tbl files."
        % args.tpch_dir
    )
    exit(1)

print("Reading and joining data...", end="", flush=True)

# Create DuckDB connection
con = duckdb.connect('../../../tpch_10/tpcdb')

# # Load tables into DuckDB as on-disk tables to handle large datasets
# for file in required_files:
#     table_name = file.split('.')[0]
#     file_path = os.path.join(args.tpch_dir, file)
#     con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}', delim='|')")

# # Perform the denormalization using temporary tables to manage memory
con.execute("DROP TABLE IF EXISTS denormalized_temp")
# Prepare the denormalized schema and get column names
schema_query = """
CREATE TEMP TABLE denormalized_chunk AS
SELECT
    l.*,
    o.*,
    c.*,
    n.*,
    r.*,
    ps.*,
    p.*,
    s.*
FROM
    lineitem l
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
LIMIT 0
"""
con.execute(schema_query)

# Get the column names of the denormalized schema
denormalized_schema = con.execute("SELECT * FROM denormalized_chunk").df().columns

# Create the output file and write the header
if args.out.endswith(".parquet"):
    print("Writing Parquet...", end="", flush=True)
else:
    print("Writing CSV...", end="", flush=True)
    with open(args.out, 'w') as out_file:
        out_file.write(','.join(denormalized_schema) + '\n')

# Process lineitem in chunks
chunk_size = 1000000  # Adjust chunk size based on your system's memory capacity
offset = 0
k=0
while True:
    print('.',end=' ')
    try:
        denormalized_chunk = con.execute(f"""
        SELECT
            l.*,
            o.*,
            c.*,
            n.*,
            r.*,
            ps.*,
            p.*,
            s.*
        FROM
            (SELECT * FROM lineitem LIMIT {chunk_size} OFFSET {offset}) l
        JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
        JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
        JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
        JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
        JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
        """).df()

        if denormalized_chunk.empty:
            break

        if args.out.endswith(".parquet"):
            denormalized_chunk.to_parquet(f'{args.out}/{k}.parquet', index=False)
        else:
            denormalized_chunk.to_csv(args.out, index=False, mode='a', header=False)
        k+=1
        offset += chunk_size

    except duckdb.CatalogException:
        break

print(" [done]")