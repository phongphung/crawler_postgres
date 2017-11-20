# from PostgreSQL import PostgresQueue
from Feedfinder import FeedFinder
from PageInfoFinder import PageInfoFinder
from Util import *
import pandas as pd
from local_var import *


class WebCrawler:
    def __init__(self):
        self.version = VERSION
        self.links = []
        self.feed_finder = FeedFinder()
        self.page_info_finder = PageInfoFinder()
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
            return COMPLETE
        else:
            # self.db.error(url, domain)
            return ERROR