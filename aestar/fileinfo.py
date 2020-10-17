import hashlib
import os
import stat
from multiprocessing import Process
from pathlib import Path

from . import db


def checksum(file, hash=hashlib.sha1, chunksize=4096, hex=True):
    h = hash()
    with open(file, 'rb') as f:
        for chunk in iter(lambda: f.read(chunksize * h.block_size), b''):
            h.update(chunk)
    if hex:
        return h.hexdigest()
    else:
        return h.digest()


class FileInfo:
    def __init__(self, info_dict=None):
        if info_dict:
            self.info_dict = info_dict
        else:
            self.info_dict = {}

    @classmethod
    def from_file(cls, path):
        if not isinstance(path, str):
            path = path.as_posix()
        stat_result = os.stat(path)

        info_dict = {f'st_{key}': int(getattr(stat_result, f'st_{key}')) for key in db.stat_fields + ['ino']}
        info_dict['path'] = path
        if stat.S_ISREG(stat_result.st_mode):
            info_dict['sha1'] = checksum(path, hex=False)
        info_dict['is_dir'] = int(stat.S_ISDIR(stat_result.st_mode))
        return cls(info_dict)


class FileProcessor(Process):
    def __init__(self, queue, path, pattern='*'):
        super().__init__()
        self.name = f'FileProcessor for {path}'
        self.queue = queue
        self.path = Path(path)
        self.pattern = pattern

    def run(self):
        for item in self.path.rglob(self.pattern):
            try:
                self.queue.put(FileInfo.from_file(item))
            except Exception as e:
                e.filepath = item
                self.queue.put(e)
        # Sentinel value
        self.queue.put(None)
