import sqlite3
stat_fields = ['mode', 'dev', 'nlink', 'uid', 'gid', 'size', 'atime', 'mtime', 'ctime']

def create_tables(cursor):
    stat = [f"`st_{field}` INTEGER" for field in stat_fields]
    delim = ',\n'
    sql = f"""
    CREATE TABLE IF NOT EXISTS `files` (
        `id` INTEGER,
        `filepath`	TEXT NOT NULL,
        `st_ino`	INTEGER NOT NULL,
        `sha1`	BLOB,
        {delim.join(stat)},
        PRIMARY KEY(`id`),
        UNIQUE(`filepath`, `st_ino`, `sha1`)
    );
    CREATE INDEX IF NOT EXISTS file_index ON files (sha1, filepath, st_ino);
    CREATE TABLE IF NOT EXISTS `volumes` (
        `voltag`	TEXT NOT NULL UNIQUE,
        `full`	INTEGER DEFAULT 0,
        `error`	INTEGER DEFAULT 0,
        `access`	INTEGER DEFAULT 1,
        `vol_bytes`	INTEGER DEFAULT 0,
        `num_tape_files`	INTEGER DEFAULT 0,
        PRIMARY KEY(`voltag`)
        );
    """
    cursor.executescript(sql)


def init_db(db_file):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    create_tables(c)
    return conn


def insert(data, table, cursor):
    keys, values = zip(*data.items())
    insert_str = "INSERT INTO {} ({}) values ({})".format(table, ",".join(keys), ",".join(['?'] * len(keys)))
    cursor.execute(insert_str, values)
