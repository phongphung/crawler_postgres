import threading
import time
from WebCrawler import WebCrawler
from PostgreSQL import PostgresQueue


WAIT_AT_END = 10


def threaded_crawler(max_threads=5):
    db = PostgresQueue()
    try:

        db.set_temp_buffers(200)
        db.execute("SET statement_timeout = '100s'", True)

        def process_queue():
            while True:
                try:
                    print('Start thread')
                    web_crawler = WebCrawler()
                    url_list = db.get_crawl_queue(10)
                    url_dict = {x: 0 for x in url_list}
                except KeyError:
                    break
                else:
                    for url in url_dict.keys():
                        url_dict[url] = web_crawler.crawl(url)

                    db.temp_update(web_crawler.result)
                    db.merge_temp_table()
                    db.update_status_crawlqueue(url_dict)

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
                threads.append(thread)
                thread.start()

        while threads:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
            time.sleep(10)

    except Exception as e:
        raise e
    finally:
        db.close()
        del db
