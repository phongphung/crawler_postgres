import pandas as pd
from PostgreSQL import PostgresQueue
from Util import *
import urltools
import io


def push_data(filename='data.csv', column='url'):
    db = PostgresQueue()
    try:
        data_file = pd.read_csv('./Data/' + filename, sep=',',  encoding = "ISO-8859-1")
        data = list(data_file[column])
        data = list(map(trim_url, data))

        f = io.StringIO()

        data = pd.DataFrame(data, columns=['_id'])
        data['status'] = 0
        data.to_csv(f, index=False)
        db.push('crawl01', 'crawlqueue', f)
        f.close()

        f = io.StringIO()
        data['url'] = data['_id']
        data['_id'] = data['_id'].apply(lambda x: urltools.extract(x).domain)
        data = data.loc[:, ['_id', 'url']]
        data.drop_duplicates(['_id'], inplace=True)
        data.fillna('', inplace=True)
        data = data.loc[data['_id']!='']
        data.to_csv(f, index=False)
        db.push('crawl01', 'data', f)
    except Exception:
        raise
    finally:
        db.close()
        del db
