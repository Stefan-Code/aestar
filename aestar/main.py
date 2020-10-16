#!/usr/bin/env python3
import hashlib
import os
import tarfile
from pathlib import Path

import click

from . import chio
from . import db





def get_import_volumes(chio_status, exclude_prefix='CLN'):
    volume_dicts = []
    for slot, info in chio_status.items():
        voltag = info.get('voltag')
        if voltag:
            if not 'FULL' in info['status']:
                raise Exception(f'Found voltag {voltag} in {slot}, but slot is not marked as FULL')
            if 'ACCESS' in info['status']:
                if not voltag.startswith(exclude_prefix):
                    volume_dicts.append({'voltag': voltag})
    return volume_dicts


def import_volumes(cursor, device=None):
    status = chio.status(device=device)
    volumes = get_import_volumes(status)
    for vol in volumes:
        db.insert(vol, 'volumes', cursor)


@click.command()
@click.argument('directory', type=click.Path(exists=True))
@click.option('--file', '-f', required=True, type=click.Path())
@click.option('--database', default='db.sqlite', type=click.Path())
def compress(directory, file, database):
    print(f'Compressing {directory}')
    root = Path(directory)
    tar = tarfile.open(file, 'w|', bufsize=512)
    connection = db.init_db(database)

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
