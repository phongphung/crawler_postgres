import multiprocessing
from ThreadedCrawler import *
import logging
import sys

logging.basicConfig(filename='phong_process.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)


def my_handler(extype, value, tb):
    logger.exception("Uncaught exception: \n Type: {0}, Value: {1}, Traceback: {2}".format(str(extype), str(value), str(tb)))


sys.excepthook = my_handler


def process_link_crawler(args, **kwargs):
    num_cpus = multiprocessing.cpu_count() * 2
    print('Run on # processes: {}'.format(num_cpus))
    processes = []
    for i in range(num_cpus):
        p = multiprocessing.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

