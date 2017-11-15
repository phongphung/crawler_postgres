import threading
import time
from PostgreSQL import PostgresQueue
from WebCrawler import WebCrawler
import pandas as pd
import logging
import sys

WAIT_AT_END = 600


logging.basicConfig(filename='phong_thread.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)


def my_handler(extype, value, tb):
    logger.exception("Uncaught exception: \n Type: {0}, Value: {1}, Traceback: {2}".
                     format(str(extype), str(value), str(tb)))


sys.excepthook = my_handler


def threaded_crawler(max_threads=5):
    db = PostgresQueue()
    try:
        db.set_temp_buffers(200)

        def process_queue():
            while True:
                try:
                    web_crawler = WebCrawler()
                    url_list = db.get_crawl_queue(200)
                    url_dict = {x: 0 for x in url_list}
                except KeyError:
                    break
                else:
                    for url in url_dict.keys():
                        url_dict[url] = web_crawler.crawl(url)
                    # web_crawler.db.conn.commit()
                    print("==============================================================")
                    print(web_crawler.result)
                    db.temp_update(web_crawler.result)
                    db.merge_temp_table()
                    db.update_status_crawlqueue(url_dict)
                    db.conn.commit()

        threads = []

        while True:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)

            time.sleep(10)
            if len(db.check_link_to_crawl()) == 0:
                print("NO MORE LINK ========================================================")
                break

            while len(threads) < max_threads:
                thread = threading.Thread(target=process_queue)
                thread.setDaemon(True)
                thread.start()
                threads.append(thread)

        while threads:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
            time.sleep(5)

        db.close()
        time.sleep(WAIT_AT_END)
    except Exception as e:
        raise e
    finally:
        db.close()
