from AddLink import *
import cProfile
from PostgreSQL import PostgresQueue
from MultiProcessingCrawler import process_link_crawler


if __name__ == '__main__':
    db = PostgresQueue()
    try:
        db.reset_outstanding()
    except Exception as e:
        print(str(e))
    finally:
        db.close()

    del db

    # push_data()
    cProfile.run('process_link_crawler(1)')

