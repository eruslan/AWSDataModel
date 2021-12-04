import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data published at S3 storage in json files into stage tables in AWS database
    DB name, user, s3 files locations are specified at dwh.cfg.
    There are to type of data to load - log data and songs data.
    All the needed queries are at copy_table_queries
    INPUT:
        cur - cursor
        conn - connection to perform commit after load compleate
    No return, No output
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables into fact and dementional tables for future use.
    There are to type of data to load - log data and songs data.
    All the needed queries are at insert_table_queries
    INPUT:
        cur - cursor
        conn - connection to perform commit after load compleate
    No return, No output
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads dwh.cfg to get connection string parameters,
    Connects to DB amd performs staging and insert parts of ETL
    Output - timing metric
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    start_log = time.perf_counter()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    elapsed_log = time.perf_counter() - start_log
    conn.close()

    print('=========================================')
    print(f'Songs import durtion {elapsed_log:0.4}')

if __name__ == "__main__":
    main()