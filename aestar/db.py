import sqlite3
stat_fields = ['mode', 'dev', 'nlink', 'uid', 'gid', 'size', 'atime', 'mtime', 'ctime']

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def create_tables(cursor):
    stat = [f"st_{field} INTEGER" for field in stat_fields]
    delim = ',\n'
    sql = f"""
    CREATE TABLE IF NOT EXISTS files (
        id      INTEGER,
        path	TEXT NOT NULL,
        st_ino	INTEGER NOT NULL,
        sha1	BLOB,
        is_dir INTEGER,
        {delim.join(stat)},
        PRIMARY KEY(id),
        UNIQUE(path, st_ino, sha1)
    );
    CREATE INDEX IF NOT EXISTS file_index ON files (sha1, path, st_ino);
    CREATE TABLE IF NOT EXISTS volumes (
        voltag	TEXT,
        full	INTEGER DEFAULT 0,
        error	INTEGER DEFAULT 0,
        access	INTEGER DEFAULT 1,
        vol_bytes	INTEGER DEFAULT 0,
        num_tape_files	INTEGER DEFAULT 0,
        PRIMARY KEY(voltag)
    );
    CREATE TABLE IF NOT EXISTS backup (
        id	INTEGER PRIMARY KEY,
        path	TEXT NOT NULL,
        absolute_path	TEXT,
        level	TEXT,
        timestamp	INTEGER
    );
    CREATE TABLE IF NOT EXISTS partial_backup (
        id	INTEGER NOT NULL,
        parent_id	INTEGER NOT NULL,
        volume	TEXT NOT NULL,
        tape_file_index	INTEGER,
        num_files	INTEGER,
        num_bytes	INTEGER,
        timestamp	INTEGER,
        timestamp_completed	INTEGER,
        FOREIGN KEY(parent_id) REFERENCES backup(id)
   );
    CREATE TABLE IF NOT EXISTS backed_up_files (
        file_id	INTEGER NOT NULL,
        partial_backup_id	INTEGER NOT NULL,
        FOREIGN KEY(partial_backup_id) REFERENCES partial_backup(id),
        FOREIGN KEY(file_id) REFERENCES files(id),
        PRIMARY KEY(file_id, partial_backup_id)
    );
    """
    logger.debug(f'Creating DB tables if they do not already exist. Using stat fields: {", ".join(stat_fields)}.')
    cursor.executescript(sql)


def init_db(db_file):
    logger.info(f'Initializing sqlite database {db_file}.')
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    create_tables(c)
    logger.debug('Done initializing sqlite database.')
    return conn


def insert(data, table, cursor, cmd='INSERT'):
    keys, values = zip(*data.items())
    insert_str = "{} INTO {} ({}) values ({})".format(cmd, table, ",".join(keys), ",".join(['?'] * len(keys)))
    cursor.execute(insert_str, values)


def select(data, table, cursor, selection='*'):
    keys, values = zip(*data.items())
    operator = 'AND'
    select_str = "SELECT {} FROM {} WHERE {}".format(selection, table, f' {operator} '.join(f'{key}=?' for key in keys))
    return cursor.execute(select_str, values)
