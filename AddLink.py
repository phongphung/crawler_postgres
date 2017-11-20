import pandas as pd
from PostgreSQL import *
from Util import *
import urltools
import io
from local_var import *


def push_data(column=COLUMN):
    db = PostgresQueue()
    try:
        # stupid
        read_data = pd.read_csv
        if FILE_TYPE.lower() == 'csv':
            pass
        elif FILE_TYPE.lower() == 'xlsx':
            read_data = pd.read_excel
        else:
            print('Wrong file type')

        with get_db_connection(db.pool) as conn:
            with get_db_cursor(conn) as cur:
                data_file = read_data('./{}/{}'.format(FOLDER, FILE_NAME), sep=SEP,  encoding=ENCODING)
                data = list(data_file[column])
                data = list(map(trim_url, data))

                f = io.StringIO()

                data = pd.DataFrame(data, columns=['_id'])
                data['status'] = OUTSTANDING
                data.to_csv(f, index=False)
                push(conn, cur, SCHEMA, CRAWL_TABLE, f)
                f.close()

                f = io.StringIO()
                data['url'] = data['_id']
                data['_id'] = data['_id'].apply(lambda x: urltools.extract(x).domain)
                data = data.loc[:, ['_id', 'url']]
                data.drop_duplicates(['_id'], inplace=True)
                data.fillna('', inplace=True)
                data = data.loc[data['_id'] != '']
                data.to_csv(f, index=False)
                push(conn, cur, SCHEMA, DATA_TABLE, f)
    except Exception:
        raise
    finally:
        db.close()
        del db
