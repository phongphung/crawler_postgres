import time

print('Init global variable')


def test(x):
    for i in range(20):
        print(x)
        time.sleep(1)


from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import register_inet, Inet
from contextlib import contextmanager
import multiprocessing as mp
import threading
import time
import psycopg2

LOCAL_DB = "dbname='crawler' user='postgres' host='localhost' password='motsach' port=5432"


@contextmanager
def get_db_cursor(pool, commit=False):
    """
    psycopg2 connection.cursor context manager.
    Creates a new cursor and closes it, commiting changes if specified.
    """
    while True:
        try:
            connection = pool.getconn()
            break
        except (psycopg2.OperationalError, psycopg2.pool.PoolError):
            # print('No connection, retry in 3s')
            time.sleep(3)
            pass

    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cursor
        if commit:
            connection.commit()
    finally:
        cursor.close()
        pool.putconn(connection)


def main():
    processes = []

    for i in range(8):
        p = mp.Process(target=thread_test, args=[i], kwargs=[])
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


def thread_test(x):
    pool = ThreadedConnectionPool(1, 5, LOCAL_DB)
    print('Created pool for process {}'.format(x))

    def threads_worker():
        for i in range(200):
            with get_db_cursor(pool) as cursor:
                print('Get conenction / cursor in 1s')
                time.sleep(1)
            time.sleep(2)
    threads = []

    for i in range(1000):
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

        time.sleep(1)

        while len(threads) < 10:
            thread = threading.Thread(target=threads_worker)
            thread.setDaemon(True)
            threads.append(thread)
            thread.start()

    while threads:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        time.sleep(1)


if __name__ == '__main__':
    main()