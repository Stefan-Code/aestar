#!/usr/bin/env python3
import os
import hashlib
import tarfile
import click
import sqlite3

from pathlib import Path
from Crypto.Cipher import AES

def checksum(file, hash_factory=hashlib.sha1, chunksize=4096, hex=True):
    file = Path(file)
    h = hash_factory()
    with file.open('rb') as f:
        for chunk in iter(lambda: f.read(chunksize * h.block_size), b''):
            h.update(chunk)
    if hex:
        return h.hexdigest()
    else:
        return h.digest()

class AESFile:
    def __init__(self, file, mode, passphrase):
        # actual file we are writing to
        # turn buffering off, as we are writing in chunks anyways
        self._file = open(file, mode=mode, buffering=0)
        # the encryption key is derived from the upper 16 bytes of the SHA256 hash (in case of AES128)
        self.key = hashlib.sha256(passphrase).digest()[:16]
        # aespipe in single-key mode uses a 0-byte IV which is incremented for each 512 byte sector 
        self.SECTOR_SIZE = 512 # bytes
        self.sector = 0
        self._cipher = self._get_cipher()
        self._encrypted_buffer = bytes(self.SECTOR_SIZE)
        
    def _next_sector(self):
        self.sector += 1
        self._cipher = self._get_cipher()
        
    def write(self, buffer):
        if len(buffer) != 512:
            raise NotImplementedError(f'Buffer Size has to be 512 bytes, not {len(buffer)}')
        # TODO: arbitrary write method
        #if len(buffer) > len(self._encrypted_buffer):
        #    self._encrypted_buffer = bytes(len(buffer))
        #written_bytes = 0
        #bytes_left = self._sector_size - self._sector_bytes 
        #print(f'we have {bytes_left} available in sector {self._sector}')
        #while written_bytes < len(buffer):
        #    write_slice = buffer[written_bytes:bytes_left]
        #    print(f'writing {len(write_slice)}')
        self._encrypted_buffer = self._cipher.encrypt(buffer)
        self._next_sector()
        return self._file.write(self._encrypted_buffer)
        
    def close(self):
        self._file.close()
        # TODO: with arbitrary write method: pad with zero bytes until the block (not sector!) is filled
        
    def _get_cipher(self):
        return AES.new(self.key, AES.MODE_CBC, IV=self.sector.to_bytes(16, byteorder='little'))
    
def init_db(db_file):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    stat_fields = ['mode', 'dev', 'nlink', 'uid', 'gid', 'size', 'atime', 'mtime', 'ctime']
    stat = [f"`st_{field}` INTEGER" for field in stat_fields]
    delim = ',\n'
    sql = f"""
    CREATE TABLE IF NOT EXISTS `files` (
        `filename`	TEXT NOT NULL,
        `st_ino`	INTEGER NOT NULL,
        `sha1`	BLOB,
        {delim.join(stat)},
        PRIMARY KEY(`filename`, `st_ino`, `sha1`)
    )
    """
    c.execute(sql)
    return conn


def insert(d, table, cursor):
    keys, values = zip(*d.items())
    insert_str = "INSERT INTO {} ({}) values ({})".format(table, ",".join(keys), ",".join(['?'] * len(keys)))
    cursor.execute(insert_str, values)


@click.command()
@click.argument('directory', type=click.Path(exists=True))
@click.option('--file', '-f', required=True, type=click.Path())
@click.option('--database', default='db.sqlite', type=click.Path())
def compress(directory, file, database):
    print(f'Compressing {directory}')
    root = Path(directory)
    tar = tarfile.open(file, 'w|', bufsize=512)
    connection = init_db(database)

    for f in root.rglob('*'):
        stats = os.stat(f)
        print(stats)
        if f.is_file():
            digest = checksum(f)
            print(f'{digest} {f}')
        else:
            print(f'{f}')
        tar.add(f, recursive=False)


if __name__ == '__main__':
    compress()
