import os
import pandas as pd

def initiate_tables(conn):
    conn.execute('DROP TABLE IF EXISTS PART;')
    conn.execute('DROP TABLE IF EXISTS PARTSUPP;')
    conn.execute('DROP TABLE IF EXISTS SUPPLIER;')
    conn.execute('DROP TABLE IF EXISTS CUSTOMER;')
    conn.execute('DROP TABLE IF EXISTS ORDERS;')
    conn.execute('DROP TABLE IF EXISTS LINEITEM;')
    conn.execute('DROP TABLE IF EXISTS NATION;')
    conn.execute('DROP TABLE IF EXISTS REGION;')

    conn.execute('''
        CREATE TABLE PART (
            P_PARTKEY       INTEGER PRIMARY KEY,
            P_NAME          VARCHAR(55),
            P_MFGR          CHAR(25),
            P_BRAND         CHAR(10),
            P_TYPE          VARCHAR(25),
            P_SIZE          INTEGER,
            P_CONTAINER     CHAR(10),
            P_RETAILPRICE   DECIMAL,
            P_COMMENT       VARCHAR(23)
        );
    ''')

    conn.execute('''
        CREATE TABLE PARTSUPP (
            PS_PARTKEY      BIGINT NOT NULL, -- references P_PARTKEY
            PS_SUPPKEY      BIGINT NOT NULL, -- references S_SUPPKEY
            PS_AVAILQTY     INTEGER,
            PS_SUPPLYCOST   DECIMAL,
            PS_COMMENT      VARCHAR(199),
            PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
        );
    ''')

    conn.execute('''
        CREATE TABLE SUPPLIER (
            S_SUPPKEY       INTEGER PRIMARY KEY,
            S_NAME          CHAR(25),
            S_ADDRESS       VARCHAR(40),
            S_NATIONKEY     BIGINT NOT NULL, -- references N_NATIONKEY
            S_PHONE         CHAR(15),
            S_ACCTBAL       DECIMAL,
            S_COMMENT       VARCHAR(101)
        );
    ''')

    conn.execute('''
        CREATE TABLE CUSTOMER (
            C_CUSTKEY       INTEGER PRIMARY KEY,
            C_NAME          VARCHAR(25),
            C_ADDRESS       VARCHAR(40),
            C_NATIONKEY     BIGINT NOT NULL, -- references N_NATIONKEY
            C_PHONE         CHAR(15),
            C_ACCTBAL       DECIMAL,
            C_MKTSEGMENT    CHAR(10),
            C_COMMENT       VARCHAR(117)
        );
    ''')

    conn.execute('''
        CREATE TABLE ORDERS (
            O_ORDERKEY      INTEGER PRIMARY KEY,
            O_CUSTKEY       BIGINT NOT NULL, -- references C_CUSTKEY
            O_ORDERSTATUS   CHAR(1),
            O_TOTALPRICE    DECIMAL,
            O_ORDERDATE     DATE,
            O_ORDERPRIORITY CHAR(15),
            O_CLERK         CHAR(15),
            O_SHIPPRIORITY  INTEGER,
            O_COMMENT       VARCHAR(79)
        );
    ''')

    conn.execute('''
        CREATE TABLE LINEITEM (
            L_ORDERKEY      BIGINT NOT NULL, -- references O_ORDERKEY
            L_PARTKEY       BIGINT NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
            L_SUPPKEY       BIGINT NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
            L_LINENUMBER    INTEGER,
            L_QUANTITY      DECIMAL,
            L_EXTENDEDPRICE DECIMAL,
            L_DISCOUNT      DECIMAL,
            L_TAX           DECIMAL,
            L_RETURNFLAG    CHAR(1),
            L_LINESTATUS    CHAR(1),
            L_SHIPDATE      DATE,
            L_COMMITDATE    DATE,
            L_RECEIPTDATE   DATE,
            L_SHIPINSTRUCT  CHAR(25),
            L_SHIPMODE      CHAR(10),
            L_COMMENT       VARCHAR(44),
            PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
        );
    ''')

    conn.execute('''
        CREATE TABLE NATION (
            N_NATIONKEY     INTEGER PRIMARY KEY,
            N_NAME          CHAR(25),
            N_REGIONKEY     BIGINT NOT NULL, -- references R_REGIONKEY
            N_COMMENT       VARCHAR(152)
        );
    ''')

    conn.execute('''
        CREATE TABLE REGION (
            R_REGIONKEY     INTEGER PRIMARY KEY,
            R_NAME          CHAR(25),
            R_COMMENT       VARCHAR(152)
        );
    ''')

# def csv_to_df(file_name, conn):
#     table_name = file_name.split('.')[0]
#     conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv('{file_name}', delim = '|', header = false, AUTO_DETECT = true);")
#     df = conn.execute(f"SELECT * FROM {table_name};").fetch_df()
#     df.to_parquet(file_name.replace(".csv", ".parquet"))

if __name__ == "__main__":
    import duckdb
    conn = duckdb.connect('../../../')
    initiate_tables(conn)
    for file_name in os.listdir():
        if file_name.endswith(".csv"):
            csv_to_df(file_name, conn)
    conn.close()