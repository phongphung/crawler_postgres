import multiprocessing as mp
from ThreadedCrawler import threaded_crawler


def process_link_crawler(args, **kwargs):
    num_processes = mp.cpu_count() * 5
    # num_processes = 1
    processes = []

    for i in range(num_processes):

        # p = mp.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        p = mp.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

