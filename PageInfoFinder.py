def handle_syntax(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SyntaxError:
            pass
        except UnicodeDecodeError:
            pass
        except AssertionError:
            pass
    return wrapper


def limit_4000(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is not None:
            return result[:4000]
        else:
            return ''
    return wrapper


def simple_replace_special_char(x):
    if x is None:
        return ''
    # x = x.replace(r"\r", '')
    # x = x.replace(r"\n", '')
    # x = x.replace(r"\t", '')
    x = x.strip()
    x = x.replace('"', '')
    return x


class PageInfoFinder(object):
    @limit_4000
    @handle_syntax
    def get_title(self, tree):
        title = ''
        temp = tree.xpath('//title')
        if temp is not None and len(temp) > 0:
            title = simple_replace_special_char(temp[0].text)
        return title

    @limit_4000
    @handle_syntax
    def get_description(self, tree):
        description = ''
        temp = tree.xpath("//meta[@property='og:description']") or tree.xpath("//meta[@name='description']")
        if temp is not None and len(temp) > 0:
            description = simple_replace_special_char(temp[0].get('content'))
        return description

    @limit_4000
    @handle_syntax
    def get_meta_twitter(self, tree):
        meta_twitter = ''
        temp = tree.xpath("//meta[@property='twitter:site']")
        if temp is not None and len(temp) > 0:
            meta_twitter = simple_replace_special_char(temp[0].get('content'))
        return meta_twitter

    @handle_syntax
    def get_locale(self, tree):
        locale = ''
        temp = tree.xpath("//meta[@property='og:locale']") \
               or tree.xpath("//meta[@name='og:locale']") \
               or tree.xpath("//meta[@name='locale']")

        if temp is not None and len(temp) > 0:
            locale = simple_replace_special_char(temp[0].get('content'))
        return locale

    @limit_4000
    @handle_syntax
    def get_meta_site_name(self, tree):
        meta_site_name = ''
        temp = tree.xpath("//meta[@property='og:site_name']") or tree.xpath("//meta[@name='keywords']")
        if temp is not None and len(temp) > 0:
            meta_site_name = simple_replace_special_char(temp[0].get('content'))
        return meta_site_name

    @limit_4000
    @handle_syntax
    def get_meta_title(self, tree):
        meta_title = ''
        temp = tree.xpath("//meta[@property='og:title']") or tree.xpath("//meta[@name='og:title']")
        if temp is not None and len(temp) > 0:
            meta_title = simple_replace_special_char(temp[0].get('content'))
        return meta_title

    @limit_4000
    @handle_syntax
    def get_meta_url(self, tree):
        meta_url = ''
        temp = tree.xpath("//meta[@property='og:url']") or tree.xpath("//meta[@name='og:url']")
        if temp is not None and len(temp) > 0:
            meta_url = simple_replace_special_char(temp[0].get('content'))
        return meta_url

    @limit_4000
    @handle_syntax
    def dummy0(self, tree):
        temp = tree.xpath('//a')
        return str(temp)[:10]

    @limit_4000
    @handle_syntax
    def dummy1(self, tree):
        return 'dummy1'

    @limit_4000
    @handle_syntax
    def dummy2(self, tree):
        return 'dummy2'

    @limit_4000
    @handle_syntax
    def dummy3(self, tree):
        return 'dummy3'

    @limit_4000
    @handle_syntax
    def dummy4(self, tree):
        return 'dummy4'

    def find_info(self, tree):
        title = self.get_title(tree)
        description = self.get_description(tree)
        meta_twitter = self.get_meta_twitter(tree)
        locale = self.get_locale(tree)
        meta_site_name = self.get_meta_site_name(tree)
        meta_title = self.get_meta_title(tree)
        meta_url = self.get_meta_url(tree)
        dummy0 = self.dummy0(tree)
        dummy1 = self.dummy1(tree)
        dummy2 = self.dummy2(tree)
        dummy3 = self.dummy3(tree)
        dummy4 = self.dummy4(tree)

        try:
            twitter_list = list(
                set(list(map(lambda x: x.get('href'), tree.xpath("//a[contains(@href, 'twitter.com/')]")))))
            twitter_list = list(map(simple_replace_special_char, twitter_list))
            twitter_list = [x for x in twitter_list if 'twitter.com/intent' not in x]
            twitter_list = [x for x in twitter_list if 'javascript:' not in x]
            twitter_list = [x for x in twitter_list if 'twitter.com/share' not in x]
            twitter_list = [x for x in twitter_list if '/status/' not in x]
            twitter_list = [x for x in twitter_list if '?' not in x]
            lang = str(list(map(lambda x: x.get('lang'), tree.xpath("//*[@lang]"))))[:4000].replace('{', '').replace('}', '')
            current_url = tree.docinfo.URL[:4000].replace('{', '').replace('}', '')
        except AssertionError:
            lang = ''
            twitter_list = []
            current_url = ''

        return {"title": title,
                "description": description,
                "meta_twitter": meta_twitter,
                "locale": locale,
                "meta_title": meta_title,
                "meta_url": meta_url,
                "meta_site_name": meta_site_name,
                "twitter_list": twitter_list,
                "url": current_url,
                "lang": lang,
                "dummy0": dummy0,
                "dummy1": dummy1,
                "dummy2": dummy2,
                "dummy3": dummy3,
                "dummy4": dummy4}
