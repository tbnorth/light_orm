"""
get_db.py - open a DB, creating if necessary

Terry N. Brown terrynbrown@gmail.com Fri 07/19/2019
"""

import os
import sqlite3
import sys
from functools import partial

try:
    from addict import Dict
except ImportError:  # pragma: no cover
    Dict = dict

try:
    import psycopg2

    psycopg2_ProgrammingError = psycopg2.ProgrammingError
except ImportError:

    class psycopg2_ProgrammingError(Exception):
        pass


paramstyle = "?"

if sys.version_info < (3, 6):
    # need dict insertion order
    print("requires Python >= 3.6")
    FileNotFoundError = IOError  # for linters
    exit(10)


def get_con_cur(dbpath, schema=None, read_only=False):
    if "dbname=" not in dbpath:
        return get_con_cur_sqlite(dbpath, schema=schema, read_only=read_only)

    return get_con_cur_psql(dbpath, schema=schema, read_only=read_only)


def get_con_cur_psql(dbpath, schema=None, read_only=False):
    con = psycopg2.connect(dbpath)
    cur = con.cursor()
    if schema:
        for sql in schema:
            if sql.lower().strip().startswith("create table"):
                table = sql.split()[2]
                break
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE " "tablename = '%s');" % table
        )
        if not cur.fetchone()[0]:
            for sql in ["begin"] + (schema or []) + ["commit"]:
                try:
                    cur.execute(sql)
                except Exception:
                    print(sql)
                    raise

    global paramstyle
    paramstyle = "%s"
    return con, cur


def get_con_cur_sqlite(filepath, schema=None, read_only=False):
    """Open DB, creating if necessary.

    Args:
        filepath (str): path to DB
        schema ([str]): list of SQL statements to create DB
        read_only (bool): open DB read only
    Returns: connection, cursor
    """
    existed = os.path.exists(filepath)
    if not existed and read_only:
        raise Exception("'%s' doesn't exist and requested read-only" % filepath)
    if read_only:
        con = sqlite3.connect("file:%s?mode=ro" % filepath, uri=True)
    else:
        con = sqlite3.connect(filepath)
    cur = con.cursor()
    if not existed:
        for sql in schema or []:
            cur.execute(sql)
        con.commit()

    return con, cur


def do_query(cur, q, vals=None):
    select = q.lower().strip().startswith("select")
    if paramstyle != "?":
        q = q.replace("?", paramstyle)  # FIXME: crude
    try:
        # vals can be a dict or list or tuple
        if (
            vals
            and isinstance(vals, (tuple, list))
            and isinstance(vals[0], (tuple, list, dict))
        ):
            cur.executemany(q, vals or [])
        else:
            cur.execute(q, vals or [])
    except Exception:
        print(q)
        print(vals)
        raise
    if not select:
        return
    try:
        res = cur.fetchall()
    except psycopg2_ProgrammingError:
        res = []
    if res and cur.description is None:
        print("\n", q, "\n")
        raise Exception(
            "Error: table defined without first field `integer primary key`?"
        )
    flds = [i[0] for i in cur.description or []]
    # this can consume a lot of RAM, but avoids blocking DB calls
    # i.e. making other queries while still consuming this result
    return [Dict(zip(flds, i)) for i in res]


def do_one(cur, q, vals=None):
    """Run a query expected to create a single record response"""
    ans = do_query(cur, q, vals=vals)
    if ans is None or len(ans) != 1:
        raise Exception("'%s' did not produce a single record response" % q)
    return ans[0]


def get_table_id(cur, table):
    """Looks at fields in table to see if `table` or `id` is used.

    Assume client code knows this, used internally.
    """
    cur.execute(f"select * from {table} limit 0")
    if any(i[0] == "id" for i in cur.description):
        return "id"
    return table


def comp(k, v):
    """SQL where elements accounting for k is null rather than null = null"""
    if v is None:
        return f"{k} is null"
    return f"{k}=?"


def get_pk(cur, table, ident, return_obj=False, multi=False, __table_id={}):

    if table not in __table_id:
        __table_id[table] = get_table_id(cur, table)
    table_id = __table_id[table]

    if ident:
        vals = " and ".join(comp(k, v) for k, v in ident.items())
        where = f" where {vals}"
        vals = [i for i in ident.values() if i is not None]
    else:
        where = ""
        vals = []

    q = f"select {table_id} from {table}{where}"
    if return_obj:
        q = q.replace(table_id, "*", 1)  # replace first <table>
    res = do_query(cur, q, vals)

    if len(res) > 1 and not multi:
        raise Exception("More than on result for %s %s" % (table, ident))
    if res:
        if multi:
            if return_obj:
                return res
            else:
                return [i[table_id] for i in res]
        else:
            if return_obj:
                return res[0]
            else:
                return res[0][table_id]
    else:
        return None


def get_rec(cur, table, ident, multi=False):
    return get_pk(cur, table, ident, return_obj=True, multi=multi)


def get_or_make_pk(cur, table, ident, defaults=None, return_obj=False):
    res = get_pk(cur, table, ident, return_obj=return_obj)
    if res:
        return res, False
    else:
        defaults = defaults.copy() if defaults else dict()
        defaults.update(ident)
        do_query(
            cur,
            "insert into {table} ({fields}) values ({values})".format(
                table=table,
                fields=",".join(defaults),
                values=",".join("?" * len(defaults)),
            ),
            list(defaults.values()),
        )
        return get_pk(cur, table, defaults, return_obj=return_obj), True


def get_pks(cur, table, ident=None, return_obj=False):
    if ident is None:
        ident = {}
    return get_pk(cur, table, ident, return_obj=return_obj, multi=True)


def get_or_make_rec(cur, table, ident, defaults=None):
    return get_or_make_pk(cur, table, ident, defaults=defaults, return_obj=True)


def get_recs(cur, table, ident=None):
    if ident is None:
        ident = {}
    return get_rec(cur, table, ident, multi=True)


def save_rec(cur, rec, table=None):
    """save_rec - save a modified record

    Args:
        opt (argparse namespace): options
        rec (Dict): record
    """
    if table is None:
        table = list(rec.keys())[0]
        table_id = table
    else:
        table_id = "id"
    pk = rec[table_id]
    vals = [(k, v) for k, v in rec.items() if k != table_id]
    q = "update {table} set {values} where {table_id} = {pk}".format(
        table=table,
        table_id=table_id,
        pk=pk,
        values=",".join("%s=?" % i[0] for i in vals),
    )
    do_query(cur, q, [i[1] for i in vals])


class LightORM:
    """Why is this class implemented this way?  Because the original use case didn't
    supply a class at all, it was assumed the user would need to use the cursor object
    anyway, so only the stand-alone functions were defined.
    """

    def __init__(self, dbpath, schema=None, read_only=False):
        self.con, self.cur = get_con_cur(dbpath, schema=schema, read_only=read_only)
        for func in (
            do_one,
            do_query,
            get_or_make_pk,
            get_or_make_rec,
            get_pk,
            get_pks,
            get_rec,
            get_recs,
            save_rec,
        ):
            setattr(self, func.__name__, partial(func, self.cur))
