"""
get_db.py - open a DB, creating if necessary

Terry N. Brown terrynbrown@gmail.com Fri 07/19/2019
"""

import os
import sqlite3
import sys
from addict import Dict

if sys.version_info < (3, 6):
    # need dict insertion order
    print("requires Python >= 3.6")
    FileNotFoundError = IOError  # for linters
    exit(10)


def get_con_cur(filepath, schema=None, read_only=False):
    """Open DB, creating if necessary.

    Args:
        filepath (str): path to DB
        schema ([str]): list of SQL statements to create DB
        read_only (bool): open DB read only
    Returns: connection, cursor
    """
    existed = os.path.exists(filepath)
    if not existed and read_only:
        raise Exception(
            "'%s' doesn't exist and requested read-only" % filepath
        )
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
    select = q.lower().strip().startswith('select')
    try:
        cur.execute(q, vals or [])
    except Exception:
        print(q)
        print(vals)
    if not select:
        return
    res = cur.fetchall()
    flds = [i[0] for i in cur.description]
    # this can consume a lot of RAM, but avoids blocking DB calls
    # i.e. making other queries while still consuming this result
    return [Dict(zip(flds, i)) for i in res]


def do_one(cur, q, vals=None):
    """Run a query expected to create a single record response"""
    ans = do_query(cur, q, vals=vals)
    if ans is None or len(ans) != 1:
        raise Exception("'%s' did not produce a single record response" % q)
    return ans[0]


def get_pk(cur, table, ident, return_obj=False, multi=False):

    q = "select {table} from {table} where {vals}".format(
        table=table, vals=' and '.join('%s=?' % k for k in ident)
    )
    if return_obj:
        q = q.replace(table, '*', 1)  # replace first <table>
    res = do_query(cur, q, list(ident.values()))

    if len(res) > 1 and not multi:
        raise Exception("More than on result for %s %s" % (table, ident))
    if res:
        if multi:
            if return_obj:
                return res
            else:
                return [i[table] for i in res]
        else:
            if return_obj:
                return res[0]
            else:
                return res[0][table]
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
            'insert into {table} ({fields}) values ({values})'.format(
                table=table,
                fields=','.join(defaults),
                values=','.join('?' * len(defaults)),
            ),
            list(defaults.values()),
        )
        return get_pk(cur, table, defaults, return_obj=return_obj), True


def get_pks(cur, table, ident, return_obj=False):
    return get_pk(cur, table, ident, return_obj=return_obj, multi=True)


def get_or_make_rec(cur, table, ident, defaults=None):
    return get_or_make_pk(
        cur, table, ident, defaults=defaults, return_obj=True
    )


def get_recs(cur, table, ident):
    return get_rec(cur, table, ident, multi=True)




def save_rec(opt, rec):
    """save_rec - save a modified record

    Args:
        opt (argparse namespace): options
        rec (Dict): record
    """
    table = list(rec.keys())[0]
    pk = rec[table]
    vals = [(k, v) for k, v in rec.items() if k != table]
    q = 'update {table} set {values} where {table} = {pk}'.format(
        table=table, pk=pk, values=','.join('%s=?' % i[0] for i in vals)
    )
    do_query(opt, q, [i[1] for i in vals])