#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six.moves.urllib import parse as urlparse
import requests
import logging


def is_feed_data(text):
    data = text.lower()
    if data.count("<html"):
        return False
    return data.count("<rss") + data.count("<rdf") + data.count("<feed")


def is_feed_url(url):
    return any(map(url.lower().endswith,
                   [".rss", ".rdf", ".xml", ".atom"]))


def is_feedlike_url(url):
    return any(map(url.lower().count,
                   ["rss", "rdf", "xml", "atom", "feed"]))


class FeedFinder(object):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                 ' Chrome/61.0.3163.100 Safari/537.36'
    timeout = 10

    def __init__(self):
        self.nothing = ''

    def is_feed(self, url):
        text = self.get_feed(url)
        if text is None:
            return False
        return is_feed_data(text)

    def get_feed(self, url):
        try:
            r = requests.get(url, headers={"User-Agent": self.user_agent}, timeout=self.timeout)
        except Exception as e:
            logging.warning("Error while getting '{0}'".format(url))
            logging.warning("{0}".format(e))
            return None
        return r.text

    def find_feeds(self, tree, url, exhaust=False):
        rss_list = []
        try:
            for link in tree.xpath("//a"):
                if link.get("type") in ["application/rss+xml",
                                        "text/xml",
                                        "application/atom+xml",
                                        "application/x.atom+xml",
                                        "application/x-atom+xml"]:
                    rss_list.append(urlparse.urljoin(url, link.get("href", "")))
            for link in tree.xpath("//link"):
                if link.get("type") in ["application/rss+xml",
                                        "text/xml",
                                        "application/atom+xml",
                                        "application/x.atom+xml",
                                        "application/x-atom+xml"]:
                    rss_list.append(urlparse.urljoin(url, link.get("href", "")))

            # Look for <a> tags.
            if exhaust:
                local, remote = [], []
                for a in tree.xpath("a"):
                    href = a.get("href", None)
                    if href is None:
                        continue
                    if "://" not in href and is_feed_url(href):
                        local.append(href)
                    if is_feedlike_url(href):
                        remote.append(href)

                local = [urlparse.urljoin(url, l) for l in local]
                rss_list += list(filter(self.is_feed, local))
                logging.info("Found {0} local <a> links to feeds.".format(len(rss_list)))

                # Check the remote URLs.
                remote = [urlparse.urljoin(url, l) for l in remote]
                rss_list += list(filter(self.is_feed, remote))
                logging.info("Found {0} remote <a> links to feeds.".format(len(rss_list)))
                # Guessing potential URLs.
                fns = ["atom.xml", "index.atom", "index.rdf", "rss.xml", "index.xml",
                       "index.rss"]
                additional = [urlparse.urljoin(url, f) for f in fns]
                rss_list += list(filter(self.is_feed, additional))
        except AssertionError:
            pass
        rss_list = list(set(rss_list))
        return rss_list
