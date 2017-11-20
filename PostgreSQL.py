import io
import psycopg2 as db
from SQLFunction import *
from psycopg2.extensions import QueryCanceledError
import time
import csv
from psycopg2.pool import ThreadedConnectionPool, PoolError
from contextlib import contextmanager
import psycopg2.extras

LOCAL_DB = "dbname='crawler' user='postgres' host='localhost' password='motsach' port=5432"
CRAWL_TABLE = 'crawlQueue'
DATA_TABLE = 'data'
SCHEMA = 'crawl01'
TEMP_DATA_TABLE = 'tmp_data'


def query_timeout(func, conn, description='No Desc', retries=3):
    def func_wrapper(*args, **kwargs):
        _retries = retries
        _desc = description
        while _retries > 0:
            try:
                return func(*args, **kwargs)
            except QueryCanceledError:
                print('Retries {} for {}'.format(_retries, _desc))
                _retries -= 1
                conn.rollback()
    return func_wrapper


def retry_connect(func, wait=10):
    def func_wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except (db.OperationalError, PoolError):
                # print('Out of connection, retry in 10s')
                time.sleep(wait)
    return func_wrapper


@contextmanager
def get_db_connection(pool):
    """
    psycopg2 connection context manager.
    Fetch a connection from the connection pool and release it.
    """
    connection = None
    try:
        connection = pool.getconn()
        # print('Got connection')
        yield connection
    except (db.OperationalError, PoolError) as e:
        raise e
    finally:
        if connection is not None:
            pool.putconn(connection)


@contextmanager
def get_db_cursor(connection):
    """
    psycopg2 connection.cursor context manager.
    Creates a new cursor and closes it, commiting changes if specified.
    """

    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cursor
    finally:
        cursor.close()


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

    def __init__(self, timeout=300, schema=SCHEMA):
        self.timeout = timeout
        self.schema = schema
        self.pool = ThreadedConnectionPool(1, 3, LOCAL_DB)
        print('Created_pool')
        self.check_storage()
        self.create_temp_table(TEMP_DATA_TABLE)

    @retry_connect
    def execute(self, query, commit=False):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query_timeout(cur.execute, conn)(query)
                if commit:
                    conn.commit()

    @retry_connect
    def set_temp_buffers(self, mb):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query_timeout(cur.execute, conn)("SET temp_buffers = '{}MB'".format(mb))
                conn.commit()

    @retry_connect
    def create_temp_table(self, name=TEMP_DATA_TABLE):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query = sql_query_create_data_temp_table(name)
                query_timeout(cur.execute, conn)(query)

    @retry_connect
    def reset_temp_table(self, name=TEMP_DATA_TABLE):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query = sql_query_drop_table(name)
                query_timeout(cur.execute, conn)(query)
                self.create_temp_table()

    @retry_connect
    def check_storage(self):
        with get_db_connection(self.pool) as conn, get_db_cursor(conn) as cur:
            query_timeout(cur.execute, conn)(sql_query_schema_exists(self.schema))
            conn.commit()

            # Check tables
            query_exists = sql_query_table_exists(self.schema, DATA_TABLE)
            query_timeout(cur.execute, conn)(query_exists)
            temp = cur.fetchone().get('exists')

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
                query_timeout(cur.execute, conn)(query)

            query_exists = sql_query_table_exists(self.schema, CRAWL_TABLE)
            query_timeout(cur.execute, conn)(query_exists)
            temp = cur.fetchone().get('exists')
            if not temp:
                query = """
                    CREATE TABLE {}.{} (
                      id SERIAL PRIMARY KEY,
                      _id VARCHAR(5000),
                      status NUMERIC,
                      timestamp TIMESTAMP
                    )
                """.format(self.schema, CRAWL_TABLE)
                query_timeout(cur.execute, conn)(query)

                query = """
                    CREATE INDEX crawlqueue_index_status ON {}.{}(status ASC)
                """.format(self.schema, CRAWL_TABLE.lower())
                query_timeout(cur.execute, conn)(query)

                query = """
                    CREATE INDEX crawlqueue_index_url ON {}.{}(_id ASC)
                """.format(self.schema, CRAWL_TABLE.lower())
                query_timeout(cur.execute, conn)(query)

                conn.commit()

    def close(self):
        self.pool.closeall()

    @retry_connect
    def error(self, url, domain):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query = """
                            UPDATE {}.{}
                            SET error = array_append(error, '{}')
                            WHERE _id = '{}'
                        """.format(self.schema, DATA_TABLE, url, domain)
                try:
                    query_timeout(cur.execute, conn)(query)
                except db.ProgrammingError:
                    conn.rollback()
                    time.sleep(5)
                    query_timeout(cur.execute, conn)(query)
                conn.commit()

    @retry_connect
    def __nonzero__(self):
        with get_db_connection(self.pool) as conn, get_db_cursor(conn) as cur:
            query_timeout(cur.execute("""
                            SELECT EXISTS(SELECT * FROM {}.{} WHERE status != {})
                        """.format(self.schema, CRAWL_TABLE, self.COMPLETE)), conn)
        return cur.fetchone().get('exists')

    @retry_connect
    def push(self, schema=SCHEMA, table_name=DATA_TABLE, file_object=None):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
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

                    query_timeout(cur.copy_expert, conn, description='Push copy')(sql=query, file=file_object)
                    conn.commit()
                    cur.close()

    @retry_connect
    def get_crawl_queue(self, limit):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                record = sql_query_check_not_crawled_execute(self.schema, CRAWL_TABLE, cur, limit)

                if len(record) != 0:
                    conn.commit()
                    return list(map(lambda x: x.get('_id'), record))
                else:
                    raise KeyError()

    @retry_connect
    def check_link_to_crawl(self):
        with get_db_connection(self.pool) as conn, get_db_cursor(conn) as cur:
            query = """
                SELECT 1 FROM {}.{} WHERE status = 0 LIMIT 1
            """.format(self.schema, CRAWL_TABLE)

            query_timeout(cur.execute, conn)(query)
            temp = cur.fetchall()
        return temp

    @retry_connect
    def reset_outstanding(self):
        query = """
            UPDATE {}.{} SET status = {} WHERE status = {}
        """.format(self.schema, CRAWL_TABLE, self.OUTSTANDING, self.PROCESSING)
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query_timeout(cur.execute, conn)(query)
                conn.commit()

    @retry_connect
    def error_crawlqueue(self, url):
        query = """
                    UPDATE {}.{} SET status = {} WHERE _id = '{}'
                """.format(self.schema, CRAWL_TABLE, self.ERROR, str(url))

        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
                query_timeout(cur.execute, conn)(query)
                conn.commit()

    @retry_connect
    def update_status_crawlqueue(self, url_dict):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
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
                    query_timeout(cur.execute, conn)(query)
                conn.commit()

    @retry_connect
    def temp_update(self, data):
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

    @retry_connect
    def merge_temp_table(self):
        with get_db_connection(self.pool) as conn:
            with get_db_cursor(conn) as cur:
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
                query_timeout(cur.execute, conn)(query)
                conn.commit()
