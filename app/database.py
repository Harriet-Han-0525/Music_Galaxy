from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from app.config import get_settings


def make_conninfo(database_name: str) -> str:
    settings = get_settings()
    return (
        f"host={settings['host']} "
        f"port={settings['port']} "
        f"user={settings['user']} "
        f"password={settings['password']} "
        f"dbname={database_name}"
    )


def get_project_conninfo() -> str:
    settings = get_settings()
    return make_conninfo(str(settings["project_database"]))


def get_admin_conninfo() -> str:
    settings = get_settings()
    return make_conninfo(str(settings["admin_database"]))


@contextmanager
def get_connection(project_db: bool = True):
    conninfo = get_project_conninfo() if project_db else get_admin_conninfo()
    with psycopg.connect(conninfo, row_factory=dict_row) as conn:
        yield conn
