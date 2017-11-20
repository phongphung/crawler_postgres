import multiprocessing as mp
from ThreadedCrawler import threaded_crawler
from local_var import *

def process_link_crawler(args, **kwargs):
    # PROCESSES = mp.cpu_count() * 5
    processes = []

    for i in range(PROCESSES):
        p = mp.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

