import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        drops all tables listed in drop_table_queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        creates all tables listed in create_table_queries list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads dwh.cfg to get connection string parameters,
    Connects to DB, drops all tables and creates them again. Both done with scripts from sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
