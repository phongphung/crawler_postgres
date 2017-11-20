import http.client
import urllib.request
import socket
import tldextract
import urllib.parse
import lxml.html
from local_var import *


def get_page(url, num_retries=2, user_agent=USER_AGENT):
    tree = ''
    try:
        req = urllib.request.Request(url)
        req.add_header(key='User-Agent', val=user_agent)
        f = urllib.request.urlopen(req, timeout=20)
        tree = lxml.html.parse(f)
        f.close()
    except IOError:
        print('IOError in opening {}'.format(url))
    except http.client.InvalidURL as e:
        print('Invalid URL error in opening {}'.format(url))
        if hasattr(e, 'reason'):
            print('Reason: {}'.format(str(e.reason)))
        elif hasattr(e, 'code'):
            print('Error code: {}'.format(str(e.code)))
    except http.client.IncompleteRead as e:
        f = e.partial
        tree = lxml.html.parse(f)
        f.close()
    except socket.timeout as e:
        tree = 'timeout'
    except urllib.request.URLError as e:
        print('URLError at: {}'.format(url))
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                return get_page(url, num_retries-1)
    except Exception as e:
        print('Error in: {} with error: {}'.format(url, str(e)))
    finally:
        return tree


def save_all_links_on_page(tree, main_url):

    def check_link(x):
        try:
            if x is not None and x[:11] == 'javascript:':
                if x[0] == '/':
                    x = urllib.parse.urljoin(main_url, x)
                return x
        except Exception as e:
            print(str(e))
            pass

    if tree == '' or tree == 'timeout':
        print('No page got')
        return []
    try:
        links = tree.xpath('.//a')
        links = list(map(lambda x: x.get('href'), links))
        # links = list(map(lambda x: x.get('href'), links))
        links = list(map(check_link, links))
    except AssertionError:
        links = []
    return links


def coerce_url(url):
    url = url.strip()
    url = url.replace(' ', '%20')
    if url.startswith("feed://"):
        return "http://{0}".format(url[7:])
    for proto in ["http://", "https://"]:
        if url.startswith(proto):
            return url
    return "http://{0}".format(url)


def check_filters(url, parent_url):
    check = (tldextract.extract(url).domain == tldextract.extract(parent_url).domain)
    check = check and (not ('.jpg' in url))
    # check =
    return check


def trim_url(url):
    extract_url = tldextract.extract(url)
    temp_main_url = (extract_url.domain + '.' + extract_url.suffix)
    if extract_url.subdomain != '':
        temp_main_url = extract_url.subdomain + '.' + temp_main_url
    temp_main_url = coerce_url(temp_main_url)
    return temp_main_url


def get_domain(url):
    try:
        url = trim_url(url)
        return tldextract.extract(url).domain
    except Exception as e:
        return str(e)
