#!/usr/bin/env python3
#
# Based on: https://github.com/Jibbow/denormalized-tpch
#
# Example: ./denormalize.py ./sf1 --out ./sf1/denormalized.csv
#

import os
print(1)
import argparse
print(2)
import modin.config
print(3)
import modin.pandas as pd
print(4)
# import ray
print(5)
# ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})
print(6)
# Change the number of cores used by Modin to avoid overloading the machine.
modin.config.CpuCount.put(1)
print(1)
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
    "to write CSN file and .parquet extension to write Parquet file.",
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
print(2)

# Check that all .tbl files generated by TPC-H tool are present.
tbl_files = os.listdir(args.tpch_dir)
if not all(
    f in tbl_files
    for f in [
        args.customer,
        args.lineitem,
        args.nation,
        args.orders,
        args.part,
        args.partsupp,
        args.region,
        args.supplier,
    ]
):
    print(
        "The specified directory %s does not contain the required .tbl files."
        % args.tpch_dir
    )
    exit(1)


print("Reading data...", end="", flush=True)

# import ray
# ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})

lineitem_path = os.path.join(args.tpch_dir, args.lineitem)
lineitem = pd.read_table(
    lineitem_path,
    sep="|",
    index_col=False,
    names=[
        "L_ORDERKEY",
        "L_PARTKEY",
        "L_SUPPKEY",
        "L_LINENUMBER",
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "L_DISCOUNT",
        "L_TAX",
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "L_SHIPDATE",
        "L_COMMITDATE",
        "L_RECEIPTDATE",
        "L_SHIPINSTRUCT",
        "L_SHIPMODE",
        "L_COMMENT",
    ],
)
orders_path = os.path.join(args.tpch_dir, args.orders)
orders = pd.read_table(
    orders_path,
    sep="|",
    index_col=False,
    names=[
        "O_ORDERKEY",
        "O_CUSTKEY",
        "O_ORDERSTATUS",
        "O_TOTALPRICE",
        "O_ORDERDATE",
        "O_ORDERPRIORITY",
        "O_CLERK",
        "O_SHIPPRIORITY",
        "O_COMMENT",
    ],
)
customer_path = os.path.join(args.tpch_dir, args.customer)
customer = pd.read_table(
    customer_path,
    sep="|",
    index_col=False,
    names=[
        "C_CUSTKEY",
        "C_NAME",
        "C_ADDRESS",
        "C_NATIONKEY",
        "C_PHONE",
        "C_ACCTBAL",
        "C_MKTSEGMENT",
        "C_COMMENT",
    ],
)
nation_path = os.path.join(args.tpch_dir, args.nation)
nation = pd.read_table(
    nation_path,
    sep="|",
    index_col=False,
    names=["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
)
region_path = os.path.join(args.tpch_dir, args.region)
region = pd.read_table(
    region_path, sep="|", index_col=False, names=["R_REGIONKEY", "R_NAME", "R_COMMENT"]
)
partsupp_path = os.path.join(args.tpch_dir, args.partsupp)
partsupp = pd.read_table(
    partsupp_path,
    sep="|",
    index_col=False,
    names=["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"],
)
part_path = os.path.join(args.tpch_dir, args.part)
part = pd.read_table(
    part_path,
    sep="|",
    index_col=False,
    names=[
        "P_PARTKEY",
        "P_NAME",
        "P_MFGR",
        "P_BRAND",
        "P_TYPE",
        "P_SIZE",
        "P_CONTAINER",
        "P_RETAILPRICE",
        "P_COMMENT",
    ],
)
supplier_path = os.path.join(args.tpch_dir, args.supplier)
supplier = pd.read_table(
    supplier_path,
    sep="|",
    index_col=False,
    names=[
        "S_SUPPKEY",
        "S_NAME",
        "S_ADDRESS",
        "S_NATIONKEY",
        "S_PHONE",
        "S_ACCTBAL",
        "S_COMMENT",
    ],
)

print(" [done]")

print("Joining tables...", end="", flush=True)

nation_region = pd.merge(
    nation,
    region,
    left_on=["N_REGIONKEY"],
    right_on=["R_REGIONKEY"],
    sort=False,
    how="inner",
)
customer_nation = pd.merge(
    customer,
    nation_region,
    left_on=["C_NATIONKEY"],
    right_on=["N_NATIONKEY"],
    sort=False,
    how="inner",
)
supplier_nation = pd.merge(
    supplier,
    nation_region,
    left_on=["S_NATIONKEY"],
    right_on=["N_NATIONKEY"],
    sort=False,
    how="inner",
)
partsupp_part = pd.merge(
    partsupp,
    part,
    left_on=["PS_PARTKEY"],
    right_on=["P_PARTKEY"],
    sort=False,
    how="inner",
)
partsupp_part_supp = pd.merge(
    partsupp_part,
    supplier_nation,
    left_on=["PS_SUPPKEY"],
    right_on=["S_SUPPKEY"],
    sort=False,
    how="inner",
)
orders_customer = pd.merge(
    orders,
    customer_nation,
    left_on=["O_CUSTKEY"],
    right_on=["C_CUSTKEY"],
    sort=False,
    how="inner",
)
lineitem_orders = pd.merge(
    lineitem,
    orders_customer,
    left_on=["L_ORDERKEY"],
    right_on=["O_ORDERKEY"],
    sort=False,
    how="inner",
)
lineitem_orders_partsupp = pd.merge(
    lineitem_orders,
    partsupp_part_supp,
    left_on=["L_PARTKEY", "L_SUPPKEY"],
    right_on=["PS_PARTKEY", "PS_SUPPKEY"],
    sort=False,
    how="inner",
    suffixes=("_CUST", "_SUPP"),
)

print(" [done]")

denormalized_tpch = lineitem_orders_partsupp

print("=====")
print("Denormalized schema:")
print("\n".join(list(denormalized_tpch.columns)))
print("=====")

if args.out.endswith(".parquet"):
    print("Writing Parquet...", end="", flush=True)
    denormalized_tpch.to_parquet(args.out, index=False)
else:
    print("Writing CSV...", end="", flush=True)
    denormalized_tpch.to_csv(args.out, index=False)

print(" [done]")
