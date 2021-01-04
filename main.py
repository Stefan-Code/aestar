#!/usr/bin/env python3
import errno
import time
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path

import click
from tqdm import tqdm

from aestar import chio
from aestar import database
from aestar.aestar import AESTarFile, PendingQueue
from aestar.fileinfo import FileProcessor

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def save_to_archive(pending_queue, archive, pre_add_callback=None, commit_callback=None):
    for item in iter(pending_queue.get, None):
        # insert the new file into the database
        # if it exists, update the record, because e.g. the mtime might have been changed
        logger.debug(f'Adding {item} to archive')
        prev_committed = archive.num_committed
        if pre_add_callback:
            # if the return value evaluates to True, the current item is skipped
            if pre_add_callback(item):
                logger.debug(f'Skipping {item} because of insert callback result.')
                # skip current item
                continue
        try:
            archive.add(item.info_dict['path'])
        except OSError as e:
            if e.errno != errno.ENOSPC:
                logger.error(f'Could not write {item}, got OSError {e.errno}.')
                raise e
            if pending_queue.restore:
                logger.critical(f'FATAL EOT while restoring file queue at item {item}')
                raise Exception('FATAL EOT while restoring the file queue.')
            logger.info(f'EOT, Could not add {item}. {len(archive.pending_files)} file(s) were still pending and are not written.')
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
        database.insert(vol, 'volumes', cursor)


def next_volume():
    print('Volume Change!')
    time.sleep(2)


@click.command()
@click.argument('directory', required=True, type=click.Path(exists=True))
@click.option('--file', '-f', required=True, type=click.Path())
@click.option('--passphrase-file', '-P', required=True, type=click.Path(exists=True))
@click.option('--database-file', default='catalogue.sqlite', type=click.Path())
@click.option('--compression', '-z', default='')
@click.option('-v', '--verbose', count=True)
def backup(directory, file, database_file, passphrase_file, compression, verbose):
    if verbose > 1:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose:
        logging.basicConfig(level=logging.INFO)
    logger.info(f'Gathering files from {directory}')
    with open(passphrase_file, 'rb') as f:
        passphrase = f.readline().strip()
    logger.debug(f'Passphrase is {passphrase} from {passphrase_file}')
    root = Path(directory)
    if not root.is_absolute():
        raise ValueError(f'Backup directory {directory} has to be given as an absolute path.')
    db = database.BackupDatabase(database_file)
    backup_id = db.create_backup(root, level='full')
    file_queue = Queue()
    processor = FileProcessor(file_queue, root)
    logger.debug('Starting FileProcessor')
    processor.start()
    queue = PendingQueue(file_queue)

    archive = AESTarFile(file, passphrase, mode='wb', compression=compression)
    volume_name = 'ABCDTEST'

    def insert_callback(item):
        # this implementation ignores metadata changes
        # which are not inserted in the catalog unless the file hash, inode or path to the file changes
        # files could also be inserted earlier by the FileFilter
        row_id = db.insert(item.info_dict, 'files', cmd='INSERT OR IGNORE').lastrowid
        logger.info(f'Inserted file {item} with id {row_id}')
        num_files_bar.total = len(queue)
        num_files_bar.update(1)
        written_bytes_bar.update()
        # check if the file is in backed_up_files not if it is inserted in files!
        # return True if not rowcount else False

    def commit_callback(item):
        file_id = db.select(item.info_dict, 'files', selection='id').fetchone()['id']
        d = {'file_id': file_id, 'partial_backup_id': partial_backup_id}
        db.insert(d, 'backed_up_files')

    def check_skip(item):
        # if level is not full:
        # return True if item in files and backed_up_files
        # if deduplication is enabled:
        # return True if sha1 in files and that file in backed_up_files
        pass

    #  commit the partial backup before starting to add files
    db.commit()
    written_bytes_bar = tqdm(position=0)
    num_files_bar = tqdm(total=queue.qsize(), position=1)
    archive_save_result = 1
    while archive_save_result:
        partial_backup_id = db.create_partial_backup(backup_id, volume_name)
        print(partial_backup_id)
        archive_save_result = save_to_archive(queue, archive,
                    pre_add_callback=insert_callback,
                    commit_callback=commit_callback
                    )
        if archive_save_result:
            next_volume()
            # open the archive again with the new volume
            archive = AESTarFile(file, passphrase, mode='wb', compression=compression)

    db.commit()
    archive.close()


if __name__ == '__main__':
    backup()
