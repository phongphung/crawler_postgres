import io
from datetime import datetime, timedelta
import psycopg2 as db
from SQLFunction import *
from psycopg2 import sql
import time
import yaml
import csv

LOCAL_DB = "dbname='crawler' user='postgres' host='localhost' password='motsach' port=5432"
CRAWL_TABLE = 'crawlQueue'
DATA_TABLE = 'data'
SCHEMA = 'crawl01'
TEMP_DATA_TABLE = 'tmp_data'


def connect_postgres(db_info=LOCAL_DB):
    try:
        conn = db.connect(db_info)
        print("Initialised Connection")
        return conn
    except Exception as e:
        print("Error db connection: " + str(e))


def fetch_result():
    return


def replace_array_postgres(x):
    x = str(list(x))
    x = x.replace('}', '')
    x = x.replace('{', '')
    x = x.replace('"', '')

    if x[0] == '[':
        x = '{' + x[1:]
    if x[-1] == ']':
        x = x[:-1] + '}'
    return x


class PostgresQueue:
    # download states
    OUTSTANDING, PROCESSING, COMPLETE, ERROR = range(4)

    def __init__(self, conn=None, timeout=300, schema=SCHEMA):
        self.timeout = timeout
        self.schema = schema
        if conn is None:
            self.conn = connect_postgres()
            self.check_storage()
            self.create_temp_table(TEMP_DATA_TABLE)
        else:
            self.conn = conn

    def set_temp_buffers(self, mb):
        cur = self.conn.cursor()
        cur.execute("SET temp_buffers = '{}MB'".format(mb))
        self.conn.commit()

    def create_temp_table(self, name=TEMP_DATA_TABLE):
        cur = self.conn.cursor()
        query = sql_query_create_data_temp_table(name)
        cur.execute(query)

    def reset_temp_table(self, name=TEMP_DATA_TABLE):
        cur = self.conn.cursor()
        query = sql_query_drop_table(name)
        cur.execute(query)
        self.create_temp_table()

    def reset_conn(self):
        self.conn = connect_postgres()

    def check_storage(self):
        # Cursor
        cur = self.conn.cursor()

        # Check schema
        cur.execute(sql_query_schema_exists(self.schema))
        self.conn.commit()

        # Check tables
        query_exists = sql_query_table_exists(self.schema, DATA_TABLE)
        cur.execute(query_exists)
        temp = cur.fetchone()[0]
        if not temp:
            query = """
                CREATE TABLE {}.{} (
                  _id VARCHAR(5000) PRIMARY KEY,
                  original_url VARCHAR(5000),
                  title VARCHAR(5000),
                  description VARCHAR(5000),
                  meta_twitter VARCHAR(5000),
                  locale VARCHAR(255),
                  meta_title VARCHAR(5000),
                  meta_url VARCHAR(5000),
                  meta_site_name VARCHAR(5000),
                  twitter_list text[],
                  rss text[],
                  url VARCHAR(5000),
                  lang VARCHAR(5000),
                  error text[]
                )
            """.format(self.schema, DATA_TABLE)
            cur.execute(query)

        query_exists = sql_query_table_exists(self.schema, CRAWL_TABLE)
        cur.execute(query_exists)
        temp = cur.fetchone()[0]
        if not temp:
            query = """
                CREATE TABLE {}.{} (
                  id SERIAL PRIMARY KEY,
                  _id VARCHAR(5000),
                  status NUMERIC,
                  timestamp TIMESTAMP
                )
            """.format(self.schema, CRAWL_TABLE)
            cur.execute(query)

            query = """
                CREATE INDEX crawlqueue_index_status ON {}.{}(status ASC)
            """.format(self.schema, CRAWL_TABLE.lower())
            cur.execute(query)

            query = """
                CREATE INDEX crawlqueue_index_url ON {}.{}(_id ASC)
            """.format(self.schema, CRAWL_TABLE.lower())
            cur.execute(query)

            self.conn.commit()

    def close(self):
        self.conn.close()

    def error(self, url, domain):
        cur = self.conn.cursor()
        query = """
                    UPDATE {}.{}
                    SET error = array_append(error, '{}')
                    WHERE _id = '{}'
                """.format(self.schema, DATA_TABLE, url, domain)
        try:
            cur.execute(query)
        except db.ProgrammingError:
            self.conn.rollback()
            time.sleep(5)
            cur.execute(query)

    # def update_result_not_commit(self, domain, url, page_info, rss):
    #     cur = self.conn.cursor()
    #     page_info['_id'] = domain
    #     page_info['rss'] = rss if rss else [0]
    #     sql_query_update_data_json_execute(self.schema, DATA_TABLE, page_info, cur, append=True)

    def __nonzero__(self):
        cur = self.conn.cursor()
        cur.execute("""
                        SELECT EXISTS(SELECT * FROM {}.{} WHERE status != {})
                    """.format(self.schema, CRAWL_TABLE, self.COMPLETE))
        return cur.fetchall()[0]

    def push(self, schema=SCHEMA, table_name=DATA_TABLE, file_object=None):
        cursor = self.conn.cursor()
        if file_object:
            file_object.seek(0)
            if schema:
                query = """
                    COPY {}.{}({}) FROM STDIN WITH
                        CSV
                        HEADER
                        DELIMITER AS ','
                """.format(schema, table_name, file_object.readline()[:-1])
            else:
                query = """
                            COPY {}({}) FROM STDIN WITH
                                CSV
                                HEADER
                                DELIMITER AS ','
                        """.format(table_name, file_object.readline()[:-1])
                print('===================================================')
                print("QUERY:" + str(query))
            file_object.seek(0)
            cursor.copy_expert(sql=query, file=file_object)
            self.conn.commit()
            cursor.close()

    def get_crawl_queue(self, limit):
        cur = self.conn.cursor()
        record = sql_query_check_not_crawled_execute(self.schema, CRAWL_TABLE, cur, limit)

        if len(record) != 0:
            self.conn.commit()
            return list(map(lambda x: x[0], record))
        else:
            raise KeyError()

    # def complete(self, url):
    #     query = """
    #         UPDATE {}.{} SET status = {} WHERE _id = '{}'
    #     """.format(self.schema, CRAWL_TABLE, self.COMPLETE, str(url))
    #
    #     cur = self.conn.cursor()
    #     cur.execute(query)
    #     self.conn.commit()

    def check_link_to_crawl(self):
        cur = self.conn.cursor()

        query = """
            SELECT 1 FROM {}.{} WHERE status = 0 LIMIT 1
        """.format(self.schema, CRAWL_TABLE)

        cur.execute(query)
        return cur.fetchall()

    def reset_outstanding(self):
        query = """
            UPDATE {}.{} SET status = {} WHERE status = {}
        """.format(self.schema, CRAWL_TABLE, self.OUTSTANDING, self.PROCESSING)
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()

    def error_crawlqueue(self, url):
        query = """
                    UPDATE {}.{} SET status = {} WHERE _id = '{}'
                """.format(self.schema, CRAWL_TABLE, self.ERROR, str(url))

        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()
        pass

    def update_status_crawlqueue(self, url_dict):
        cur = self.conn.cursor()
        status_set = set(url_dict.values())
        for status in status_set:
            url_list = [k for (k, v) in url_dict.items() if v == status]
            if len(url_list) == 1:
                temp = "('{}')".format(url_list[0])
            else:
                temp = str(tuple(url_list))
            query = """
                UPDATE {}.{} SET status = {} WHERE _id in {}
            """.format(self.schema, CRAWL_TABLE, status, temp)
            print(query)
            cur.execute(query)

        self.conn.commit()

    def temp_update(self, data):
        cur = self.conn.cursor()
        f = io.StringIO()
        data.fillna('', inplace=True)
        data.drop_duplicates(['_id'], inplace=True)
        data.reset_index(inplace=True, drop=True)

        array_list_columns = ['twitter_list', 'rss', 'error']
        columns = data.columns
        for column in array_list_columns:
            if column in columns:
                data[column] = data[column].apply(replace_array_postgres)

        data.to_csv(f, index=False, quoting=csv.QUOTE_ALL)
        self.push(schema='', table_name=TEMP_DATA_TABLE, file_object=f)

    def merge_temp_table(self):
        cur = self.conn.cursor()
        query = """
            UPDATE {0} test
            SET title = temp.title,
                description = temp.description,
                meta_twitter = temp.meta_twitter,
                locale = temp.locale,
                meta_title = temp.meta_title,
                meta_url = temp.meta_url,
                meta_site_name = temp.meta_site_name,
                twitter_list = array_cat(test.twitter_list, temp.twitter_list),
                rss = array_cat(test.rss, temp.rss),
                url = temp.url,
                lang = temp.lang,
                error = array_cat(test.error, temp.error)
            FROM {1} temp
            WHERE test._id = temp._id
        """.format(SCHEMA + '.' + DATA_TABLE, TEMP_DATA_TABLE)
        print(query)
        cur.execute(query)
