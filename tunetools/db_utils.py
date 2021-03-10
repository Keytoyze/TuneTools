import sqlite3
from typing import Iterable, Dict


def execute_sql(conn: sqlite3.Connection, sql: str, parameters: Iterable = None):
    print("Execute: %s (%s)" % (sql, parameters))
    if parameters is not None:
        return conn.execute(sql, parameters)
    else:
        return conn.execute(sql)


def execute_sql_return_first(conn: sqlite3.Connection, sql: str, parameters: Iterable = None):
    cursor = execute_sql(conn, sql, parameters)
    for x in cursor:
        return x
    return None


def execute_many(conn: sqlite3.Connection, sql: str, seq_of_parameters: list = None):
    print("Execute many: %s (%s)...[%d]" % (sql, seq_of_parameters[0], len(seq_of_parameters)))
    conn.executemany(sql, seq_of_parameters)


def create_table(conn: sqlite3.Connection, table_name: str, columns: Dict):
    sql = "CREATE TABLE IF NOT EXISTS `%s` (%s)" % (
        table_name,
        ", ".join(["%s %s" % (k, v) for k, v in columns.items()])
    )
    execute_sql(conn, sql)


def ensure_column(conn: sqlite3.Connection, table_name: str,
                  name_type_default: Iterable):
    cursor = select(conn, table_name)
    columns = [description[0] for description in cursor.description]
    for name, db_type, default in name_type_default:
        if name not in columns:
            if default is not None:
                statement = ("ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s" % (
                    table_name, name, db_type, default
                ))
            else:
                statement = ("ALTER TABLE %s ADD COLUMN %s %s" % (
                    table_name, name, db_type
                ))
            execute_sql(conn, statement)


def update(conn: sqlite3.Connection, table_name: str, put: dict,
           where: dict):
    replace_items = put.items()
    where_items = where.items()

    sql = 'UPDATE `%s`' % table_name + \
          ' SET ' + ', '.join(['%s = ?' % k for k, _ in replace_items]) + \
          ' WHERE ' + " AND ".join(['%s = ?' % k for k, _ in where_items])

    parameters = [v for _, v in replace_items] + [v for _, v in where_items]

    execute_sql(conn, sql, parameters)


def select(conn: sqlite3.Connection, table_name: str,
           project: list = None, where: dict = None,
           order_by: str = None, limit: int = None,
           return_first: bool = False):
    if where is None:
        where = {"1": 1}
    where_items = where.items()
    project = ", ".join(project) if project is not None else "*"
    sql = "SELECT %s FROM `%s`" % (project, table_name) + \
          " WHERE " + " AND ".join(['%s = ?' % k for k, _ in where_items])
    if order_by is not None:
        sql += " ORDER BY " + order_by
    if limit is not None:
        sql += " LIMIT " + str(limit)

    parameters = [v for _, v in where_items]
    if return_first:
        return execute_sql_return_first(conn, sql, parameters)
    else:
        return execute_sql(conn, sql, parameters)


def select_first(conn: sqlite3.Connection, table_name: str,
                 project: list = None, where: dict = None,
                 order_by: str = None, limit: int = None):
    cursor = select(conn, table_name, project, where, order_by, limit)
    for x in cursor:
        return x
    return None


def count(conn: sqlite3.Connection, table_name: str, where: dict = None):
    return select_first(conn, table_name, project=["COUNT(*)"], where=where)[0]


def insert(conn: sqlite3.Connection, table_name: str, contents: list):
    columns = contents[0].keys()
    sql = "INSERT INTO `%s` (%s) VALUES (%s)" % (
        table_name, ", ".join(columns),
        ", ".join(["?"] * (len(columns)))
    )
    seq_of_params = [
        [model[column] for column in columns]
        for model in contents
    ]
    execute_many(conn, sql, seq_of_params)
