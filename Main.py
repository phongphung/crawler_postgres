from AddLink import *
import cProfile
from Util import check_init
from MultiProcessingCrawler import process_link_crawler
from local_var import *

if __name__ == '__main__':
    check_init()
    push_data()
    cProfile.run('process_link_crawler({})'.format(THREADS))
