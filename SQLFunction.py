from psycopg2 import sql
from datetime import datetime


def sql_query_table_exists(schema, table_name):
    return "select exists(select * from pg_tables where schemaname= '{}' and tablename = '{}')".format(schema.lower(),
                                                                                                       table_name.lower())


def sql_query_schema_exists(schema_name):
    return """
    DO $$
    BEGIN

        IF NOT EXISTS(
            SELECT schema_name
              FROM information_schema.schemata
              WHERE schema_name = '{0}'
          )
        THEN
            EXECUTE 'CREATE SCHEMA {0}';
        END IF;

    END
    $$;
    """.format(schema_name)


def sql_query_record_exists(schema, table, _id):
    return "SELECT _id FROM {}.{} WHERE _id = '{}' LIMIT 1".format(schema, table, _id)


def sql_query_insert_data_json_execute(schema, table, data_json, cur):
    query = sql.SQL("INSERT INTO {}.{}({{}}) VALUES({{}})".format(schema, table)).format(
        sql.SQL(', ').join(map(sql.Identifier, data_json.keys())),
        sql.SQL(', ').join(sql.Placeholder() * len(data_json))
    )
    cur.execute(query, tuple(data_json.values()))


def sql_query_update_data_json_execute(schema, table, data_json, cur, append=True):
    _id = data_json.pop('_id')

    if append:
        # Split list values to add array_cat to query
        data_list = [x for x in data_json.items() if type(x[1]) == list]
        data_normal = [x for x in data_json.items() if type(x[1]) != list]
        if len(data_normal) > 0:
            query = sql.SQL("UPDATE {}.{} SET ({{}}) = ({{}})".format(schema, table)).format(
                sql.SQL(', ').join(map(sql.Identifier, [x[0] for x in data_normal])),
                sql.SQL(', ').join(sql.Placeholder() * len(data_normal))
            )
        else:
            query = sql.SQL("UPDATE {}.{} SET".format(schema, table))
        for i in data_list:
            if query.as_string(cur)[-3:] != 'SET':
                query += sql.SQL(", {0}=array_cat({0}, {1})").format(
                    sql.Identifier(i[0]),
                    sql.Placeholder())
            else:
                query += sql.SQL(" {0}=array_cat({0}, {1})").format(
                    sql.Identifier(i[0]),
                    sql.Placeholder())
        query += sql.SQL(' WHERE _id = {}').format(sql.Placeholder())
        print('Update info: {}'.format(data_json.get('url')))
        cur.execute(query, tuple([x[1] for x in data_normal] + [x[1] for x in data_list] + [_id]))
    else:
        query = sql.SQL("UPDATE {}.{} SET ({{}}) = ({{}}) WHERE _id = {{}}".format(schema, table)).format(
            sql.SQL(', ').join(map(sql.Identifier, data_json.keys())),
            sql.SQL(', ').join(sql.Placeholder() * len(data_json))
        )
        cur.execute(query, tuple(data_json.values()) + tuple([_id]))


def sql_query_check_not_crawled_execute(schema, table, cur, limit):
    query = sql.SQL("""
                WITH temp AS(
                        SELECT id
                        FROM {0}.{1}
                        WHERE status = 0
                        LIMIT {2}
                )
                UPDATE {0}.{1} db
                    SET status = 1, timestamp = {{}}
                    FROM temp
                    WHERE db.id = temp.id
                RETURNING _id
        """.format(schema, table, limit)).format(sql.Placeholder())

    cur.execute(query, tuple([datetime.now()]))
    data = cur.fetchall()
    return data


def sql_query_create_data_temp_table(name):
    return "CREATE TEMP TABLE {} AS SELECT * FROM crawl01.data LIMIT 0".format(name)


def sql_query_drop_table(name):
    return "DROP TABLE {}".format(name)