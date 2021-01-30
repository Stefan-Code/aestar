#!/usr/bin/env python3
import logging
from multiprocessing import Queue
from pathlib import Path

import click
from tqdm import tqdm

from aestar import chio
from aestar import database
from aestar.aestar import AESTarFile, PendingQueue, save_to_archive
from aestar.fileinfo import FileProcessor, FileFilter

import uuid
import time

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Backup:
    def __init__(self, root_dir, file, database_file, passphrase, compression):
        self.root_dir = root_dir
        self.file = file
        self.passphrase = passphrase
        self.compression = compression
        self.db = database.BackupDatabase(database_file)
        self.backup_id = self.db.create_backup(root_dir, level='full')
        self.unfiltered_file_queue = Queue()
        self.file_queue = Queue()
        self.file_processor = FileProcessor(self.unfiltered_file_queue, root_dir)
        self.file_filter = FileFilter(self.unfiltered_file_queue, self.file_queue, self.filter_item)
        self.pending_queue = PendingQueue(self.file_queue)
        self.written_bytes_bar = tqdm(position=0, unit_scale=True, unit='B', miniters=1, smoothing=0)
        self.num_files_bar = tqdm(total=self.file_queue.qsize(), position=1, leave=True, miniters=1, unit='files')
        self.partial_backup_id = None  # is updated to the current id during run()
        self.archive = None
        self.i = 0

    def _setup_archive(self):
        self.archive = AESTarFile(passphrase=self.passphrase, file=self.file, mode='wb', compression=self.compression)

    def filter_item(self, item):
        return True

    def next_volume(self):
        volume_name = uuid.uuid4().hex
        print(f'Using Volume {volume_name}')
        self.partial_backup_id = self.db.create_partial_backup(self.backup_id, volume_name)
        print(self.partial_backup_id)
        self._setup_archive()

    def run(self):
        #  commit the partial backup before starting to add files
        self.next_volume()
        self.db.commit()
        self.file_processor.start()
        self.file_filter.start()

        archive_save_result = 1
        while archive_save_result:
            archive_save_result = save_to_archive(self.pending_queue, self.archive,
                                                  pre_add_callback=self.insert_callback,
                                                  commit_callback=self.commit_callback
                                                  )
            if archive_save_result:
                # open the archive again with the new volume
                self.next_volume()

        self.db.commit()
        self.archive.close()

    def insert_callback(self, item):
        # this implementation ignores metadata changes
        # which are not inserted in the catalog unless the file hash, inode or path to the file changes
        # this is by design, because otherwise the catalogue does not match the metadata in the backup on tape
        # it's not a good design though, because in case of a second full backup, the metadata in the catalogue will not be correct!
        # it would be better to just have the primary keys in `files` and metadata in `backed_up_files`
        # files could also be inserted earlier by the FileFilter (?)
        row_id = self.db.insert(item.info_dict, 'files', cmd='INSERT OR IGNORE').lastrowid
        print('row', row_id)
        item.id = row_id
        logger.info(f'Inserted file {item} with id {row_id}')
        self.num_files_bar.total = self.num_files_bar.n + self.file_queue.qsize()
        self.num_files_bar.update(1)
        self.written_bytes_bar.update(item.info_dict['st_size'])
        self.written_bytes_bar.set_postfix({'file': item.info_dict['path']})
        # check if the file is in backed_up_files not if it is inserted in files!
        # return True if not rowcount else False

    def commit_callback(self, item):
        # PROBLEM: the INSERT or IGNORE insertion might lead to an empty result
        # when querying the whole info_dict (e.g. when atime changes)
        # file_id = self.db.select(item.info_dict, 'files', selection='id').fetchone()['id']
        file_id = item.id
        print('item', item.id)
        d = {'file_id': file_id, 'partial_backup_id': self.partial_backup_id}
        self.db.insert(d, 'backed_up_files')


def import_volumes(cursor, device=None):
    status = chio.status(device=device)
    volumes = get_import_volumes(status)
    for vol in volumes:
        # TODO: abstract in db object
        database.insert(vol, 'volumes', cursor)


def check_skip(item):
    # if level is not full:
    # return True if item in files and backed_up_files
    # if deduplication is enabled:
    # return True if sha1 in files and that file in backed_up_files
    pass


@click.command()
@click.argument('directory', required=True, type=click.Path(exists=True))
@click.option('--file', '-f', required=True, type=click.Path())
@click.option('--passphrase-file', '-P', required=True, type=click.Path(exists=True))
@click.option('--database-file', default='catalogue.sqlite', type=click.Path())
@click.option('--compression', '-z', default='')
@click.option('-v', '--verbose', count=True)
@click.option('--logfile', default=None)
def do_backup(directory, file, database_file, passphrase_file, compression, verbose, logfile):
    if verbose > 1:
        logging.basicConfig(filename=logfile if logfile else None, level=logging.DEBUG)
    elif verbose:
        logging.basicConfig(filename=logfile if logfile else None, level=logging.INFO)

    logger.info(f'Backing up {directory}')

    with open(passphrase_file, 'rb') as f:
        passphrase = f.readline().strip()
    logger.debug(f'Passphrase is {passphrase} from {passphrase_file}')

    root = Path(directory)
    if not root.is_absolute():
        raise ValueError(f'Backup directory {directory} has to be given as an absolute path.')

    backup = Backup(root_dir=root, file=file, database_file=database_file, passphrase=passphrase,
                    compression=compression)
    backup.run()

    print('Done!')


if __name__ == '__main__':
    do_backup()
