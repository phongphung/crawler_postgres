# from PostgreSQL import PostgresQueue
from Feedfinder import FeedFinder
from PageInfoFinder import PageInfoFinder
from Util import *
import pandas as pd

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                 ' Chrome/61.0.3163.100 Safari/537.36'
VERSION = '1.0.0'


class WebCrawler:
    def __init__(self):
        self.version = VERSION
        self.links = []
        self.feed_finder = FeedFinder()
        self.page_info_finder = PageInfoFinder()
        # self.db = PostgresQueue()
        self.result = pd.DataFrame()

    def update_result(self, domain, url, page_info, rss):
        page_info['_id'] = domain
        page_info['rss'] = rss
        page_info['original_url'] = url
        self.result = self.result.append(pd.DataFrame([page_info], index=[0]))

    def crawl(self, url):
        print('Get: \t{}'.format(url))
        tree = get_page(url)
        main_url = trim_url(url)
        domain = get_domain(main_url)
        if type(tree) != str:
            self.links = save_all_links_on_page(tree, main_url)
            rss = self.feed_finder.find_feeds(tree, url)
            page_info = self.page_info_finder.find_info(tree)
            self.update_result(domain, url, page_info, rss)
            return 2  # COMPLETE STATUS
        else:
            # self.db.error(url, domain)
            return 4  # ERROR STATUS
