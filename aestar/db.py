import sqlite3
import logging

stat_fields = ['mode', 'dev', 'nlink', 'uid', 'gid', 'size', 'atime', 'mtime', 'ctime']

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def create_tables(cursor):
    stat = [f"st_{field} INTEGER" for field in stat_fields]
    delim = ',\n'
    # Reminder: all foreign keys have to be primary keys in the parent table
    # Foreign key support has to be explicitly enabled with PRAGMA
    # Problematic: file has unchanged content (same sha1) and path, but e.g. mtime differs
    # -> Ignore?
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
        /* absolute_path	TEXT, */
        level	TEXT,
        completed INTEGER,
        timestamp	INTEGER,
        timestamp_completed INTEGER
    );
    CREATE TABLE IF NOT EXISTS partial_backup (
        id	INTEGER NOT NULL,
        parent_id	INTEGER NOT NULL,
        volume	TEXT NOT NULL,
        tape_file_index	INTEGER,
        completed INTEGER DEFAULT 0,
        num_files	INTEGER DEFAULT 0,
        num_bytes	INTEGER DEFAULT 0,
        timestamp	INTEGER,
        timestamp_completed	INTEGER,
        FOREIGN KEY(parent_id) REFERENCES backup(id),
        PRIMARY KEY(id)
   );
    CREATE TABLE IF NOT EXISTS backed_up_files (
        file_id	INTEGER NOT NULL,
        partial_backup_id	INTEGER NOT NULL,
        deduplication_file_id INTEGER,
        FOREIGN KEY(partial_backup_id) REFERENCES partial_backup(id),
        FOREIGN KEY(file_id) REFERENCES files(id) ON UPDATE CASCADE,
        FOREIGN KEY(deduplication_file_id) REFERENCES files(id),
        PRIMARY KEY(file_id, partial_backup_id)
    );
    PRAGMA foreign_keys = ON;
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
    try:
        keys, values = zip(*data.items())
        insert_str = "{} INTO {} ({}) values ({})".format(cmd, table, ",".join(keys), ",".join(['?'] * len(keys)))
        logger.debug(f'Inserting: {insert_str} with values {values}')
        cursor.execute(insert_str, values)
    except sqlite3.DatabaseError:
        logger.error(f'Error while inserting: {insert_str} with values {values}')
        raise


def select(data, table, cursor, selection='*', chain_operator='AND'):
    keys, values = zip(*data.items())
    select_str = "SELECT {} FROM {} WHERE {}".format(selection, table, f' {chain_operator} '.join(f'{key}=?' for key in keys))
    logger.debug(f'Selecting: {select_str} with values {values}')
    return cursor.execute(select_str, values)

class BackupDatabase:
    def __init__(self, db_file):
        self.connection = init_db(db_file)

