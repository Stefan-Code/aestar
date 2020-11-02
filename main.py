#!/usr/bin/env python3
import errno
from multiprocessing import Queue
from pathlib import Path

import click
from tqdm import tqdm

from . import chio
from . import db
from .aestar import AESTarFile, PendingQueue
from .fileinfo import FileProcessor


def save_to_archive(pending_queue, archive, pre_add_callback=None, commit_callback=None):
    for item in iter(pending_queue.get, None):
        # insert the new file into the database
        # if it exists, update the record, because e.g. the mtime might have been changed
        prev_committed = archive.num_committed
        if pre_add_callback:
            pre_add_callback(item)
        try:
            archive.add(item.info_dict['path'])
        except OSError as e:
            if e.errno == errno.ENOSPC:
                print('EOT')
            print(f'Could not add {item}')
            print(f'Also, {len(archive.pending_files)} file(s) were still pending and are not written.')
            archive.close_early()
            pending_queue.restore = True
            return 1
        except Exception as e:
            # set the Queue to restore state, then let the caller handle the exception
            pending_queue.restore = True
            raise e
        finally:
            # this is executed even when we return from the function
            diff = archive.num_committed - prev_committed
            # insert successful files to sqlite
            items = pending_queue.confirm(diff)
            # print(len(items))
            if commit_callback:
                for committed in items:
                    commit_callback(committed)
    archive.close()
    return 0


def get_import_volumes(chio_status, exclude_prefix='CLN'):
    volume_dicts = []
    for slot, info in chio_status.items():
        voltag = info.get('voltag')
        if voltag:
            if not 'FULL' in info['status']:
                raise Exception(f'Found volume "{voltag}" in {slot}, but slot is not marked as FULL')
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
@click.argument('directory', required=True, type=click.Path(exists=True))
@click.option('--file', '-f', required=True, type=click.Path())
@click.option('--passphrase-file', '-P', required=True, type=click.Path(exists=True))
@click.option('--database', default='catalogue.sqlite', type=click.Path())
@click.option('--compression', '-z', default='')
def backup(directory, file, database, passphrase_file, compression):
    click.echo(f'Gathering files from {directory}')
    with passphrase_file.open('r') as f:
        passphrase = f.readline().strip()
    click.echo(f'Passphrase is {passphrase} from {passphrase_file}')
    conn = db.init_db(database)
    cursor = conn.cursor()
    print(type(directory))
    root = Path(directory)
    file_queue = Queue()
    processor = FileProcessor(file_queue, root)
    click.echo('Starting FileProcessor')
    processor.start()
    queue = PendingQueue(file_queue)

    archive = AESTarFile(file, passphrase, compression)
    partial_backup_id = 123

    with tqdm(total=file_queue.qsize()) as bar:
        def insert_callback(item):
            db.insert(item.info_dict, 'files', cursor, cmd='INSERT OR REPLACE')
            bar.total = len(queue)
            bar.update(1)

        def commit_callback(item):
            file_id = db.select(item.info_dict, 'files', cursor, selection='id').fetchone()['id']
            d = {'file_id': file_id, 'partial_backup_id': partial_backup_id}
            db.insert(d, 'backed_up_files', cursor)

        save_to_archive(queue, archive, cursor,
                        partial_backup_id=partial_backup_id,
                        pre_add_callback=insert_callback,
                        commit_callback=commit_callback
                        )


if __name__ == '__main__':
    backup()
