import yaml

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

LOCAL_DB = cfg['database']['LOCAL_DB']
CRAWL_TABLE = cfg['database']['CRAWL_TABLE']
DATA_TABLE = cfg['database']['DATA_TABLE']
SCHEMA = cfg['database']['SCHEMA']
TEMP_DATA_TABLE = cfg['database']['TEMP_DATA_TABLE']
BUFFER_SIZE = cfg['database']['BUFFER_SIZE']
TIMEOUT = cfg['database']['TIMEOUT']
RETRY_CONNECT = cfg['database']['RETRY_CONNECT']
OUTSTANDING = cfg['download']['OUTSTANDING']
PROCESSING = cfg['download']['PROCESSING']
COMPLETE = cfg['download']['COMPLETE']
ERROR = cfg['download']['ERROR']
WAIT_AT_END = cfg['other']['WAIT_AT_END']
USER_AGENT = cfg['other']['USER_AGENT']
VERSION = cfg['other']['VERSION']
FOLDER = cfg['data']['FOLDER']
FILE_NAME = cfg['data']['FILE_NAME']
FILE_TYPE = cfg['data']['FILE_TYPE']
ENCODING = cfg['data']['ENCODING']
SEP = cfg['data']['SEP']
COLUMN = cfg['data']['COLUMN']