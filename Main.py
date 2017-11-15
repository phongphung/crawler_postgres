from AddLink import *
from MultiProcessingCrawler import *
import cProfile
import logging
import sys
from PostgreSQL import PostgresQueue


logging.basicConfig(filename='phong_main.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)


def my_handler(extype, value, tb):
    logger.exception("Uncaught exception: \n Type: {0}, Value: {1}, Traceback: {2}".
                     format(str(extype), str(value), str(tb)))


sys.excepthook = my_handler


if __name__ == '__main__':
    a = PostgresQueue()
    try:
        a.reset_outstanding()
    except Exception as e:
        print(str(e))
    finally:
        a.close()

    push_data()
    cProfile.run('process_link_crawler(4)')

