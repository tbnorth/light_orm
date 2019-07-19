import light_orm
import pytest

DB_SQL = [
    """create table est (
    est integer primary key,
    site int,
    date date,
    flow real
    )""",
    """create table site (
    site integer primary key,
    name text
    )""",
    """create index site_site_idx on site(site)""",
]


@pytest.fixture
def dbpath(tmp_path_factory):
    return str(tmp_path_factory.mktemp('tmp').joinpath('some.db'))


def check_db(cur, insert=True):
    if insert:
        light_orm.get_or_make_pk(cur, 'est', {'date': 2010, 'flow': 11.2})
    res = light_orm.get_rec(cur, 'est', {'date': 2010})
    assert res == {'est': 1, 'date': 2010, 'flow': 11.2, 'site': None}


def test_open(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)
    check_db(cur)


def test_read_only(dbpath):
    with pytest.raises(Exception):
        con, cur = light_orm.get_con_cur(dbpath, DB_SQL, read_only=True)


def test_read_only_write(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)  # make the DB exist
    with pytest.raises(Exception):
        con, cur = light_orm.get_con_cur(dbpath, DB_SQL, read_only=True)
        # now save a record in read-only mode
        check_db(cur, insert=True)


def test_read_only_existing(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)
    check_db(cur)
    con.commit()
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL, read_only=True)
    check_db(cur, insert=False)


def test_get_or_make_pk(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)
    assert light_orm.get_or_make_pk(cur, 'est', {'date': 2010}) == (1, True)
    # should not insert a second copy
    assert light_orm.get_or_make_pk(cur, 'est', {'date': 2010}) == (1, False)
    assert cur.execute("select count(*) from est").fetchone()[0] == 1


def test_get_pks(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)
    light_orm.get_or_make_pk(cur, 'est', {'date': 2010, 'site': 1})
    light_orm.get_or_make_pk(cur, 'est', {'date': 2010, 'site': 2})
    assert sorted(light_orm.get_pks(cur, 'est', {'date': 2010})) == [1, 2]
    assert cur.execute("select count(*) from est").fetchone()[0] == 2


def test_get_recs(dbpath):
    con, cur = light_orm.get_con_cur(dbpath, DB_SQL)
    light_orm.get_or_make_pk(cur, 'est', {'date': 2010, 'site': 1})
    light_orm.get_or_make_pk(cur, 'est', {'date': 2010, 'site': 2})
    res = light_orm.get_recs(cur, 'est', {'date': 2010})
    res.sort(key=lambda x: x.est)
    assert res == [
        {'est': 1, 'date': 2010, 'flow': None, 'site': 1},
        {'est': 2, 'date': 2010, 'flow': None, 'site': 2},
    ]
    assert cur.execute("select count(*) from est").fetchone()[0] == 2