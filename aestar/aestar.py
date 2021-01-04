import errno
import hashlib
import os
import tarfile
import warnings
import logging

from collections import deque
from Crypto.Cipher import AES

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class TapeFile:
    def __init__(self, file=None, fileobj=None, mode='wb', bufsize=None):
        """
        File object that abstracts a tape drive (or any other file) and raises zero byte writes as an ENOSPC error.
        :param file: file location
        :param mode: file opening mode
        :param bufsize: explicit size of the underlying buffer
        """
        self.bufsize = bufsize
        if fileobj:
            self.fileobj = fileobj
        elif file and fileobj:
            raise ValueError('Arguments "file" and "fileobj" are exclusive.')
        elif file:
            self.fileobj = open(file, mode=mode, buffering=bufsize)

    def close(self):
        self.fileobj.close()

    def write(self, buf):
        result = self.fileobj.write(buf)
        if result == 0:
            # convert a write return 0 to ENOSPC to detect end of tape
            raise IOError(errno.ENOSPC, os.strerror(errno.ENOSPC))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AESFile:
    def __init__(self, passphrase, file=None, fileobj=None, mode='wb', bufsize=512, sync=True, pad=True):
        """
        Python file object for transparent AES encryption compatible with `aespipe` in single-key mode.
        This class is closely suited for the requirements of the tarfile library and not
        suited for arbitrary size write operations without respect to the buffer size.
        :param file: output file or device path to write to
        :param mode: the output file will be opened with mode
        :param passphrase: bytestring to derive the encryption key from. To be compatible with `aespipe` there may be no newline char at the end!
        :param bufsize: output buffer size used to buffer sector changes. At the end of each user write() the buffer is flushed if sync=True
        :param sync: whether to flush and fsync the buffer at the end of each write operation
        :param pad: whether to pad data to be written with zero bytes to the sector size.
                    WARNING: this is only useful if the LAST write operation does not align to the sector size
                    Otherwise there will be many zero-bytes in your written data!
        Please note that this implementation uses a constant IV for every sector to be compatible with `aespipe`.
        Therefore, it is recommended to use a different passphrase for every file to avoid leaking information!

        """
        self.SECTOR_SIZE = 512  # bytes
        if mode != 'wb':
            raise NotImplementedError("Mode must be 'wb' for now.")
        if len(passphrase) < 20:
            msg = f'Passphrase length incompatible with aespipe! {len(passphrase)}<20 characters.'
            warnings.warn(msg)
            logging.warning(msg)
        if bufsize < self.SECTOR_SIZE:
            raise ValueError(f'Buffer Size has to be at least {self.SECTOR_SIZE} bytes, not {bufsize}')
        elif bufsize % self.SECTOR_SIZE:
            raise ValueError(f'Buffer Size has to be a multiple of {self.SECTOR_SIZE} bytes, not {bufsize}')
        self.bufsize = bufsize
        self.sync = sync
        self.pad = pad
        # the encryption key is derived from the upper 16 bytes of the SHA256 hash (in case of AES128)
        self.key = hashlib.sha256(passphrase).digest()[:16]

        # aespipe in single-key mode uses a 0-byte IV which is incremented for each 512 byte sector
        self.sector = 0
        self.bytes = 0
        self._cipher = self._get_cipher()
        self._encrypted_buffer = bytes(self.bufsize)

        # actual file we are writing to
        # turn buffering off, as we are writing buffered chunks anyways
        # note that the buffer is explicitly flushed after each write()
        if file and fileobj:
            raise ValueError('Arguments "file" and "fileobj" are exclusive.')
        elif fileobj:
            self.fileobj = fileobj
        elif file:
            self.fileobj = open(file, mode=mode, buffering=self.bufsize)
        else:
            raise ValueError('Either file or fileobj is required.')
        logger.debug(f'opened {file} with buffer size {self.bufsize} for writing AES encrypted data.')

    def _next_sector(self):
        self.sector += 1
        self._cipher = self._get_cipher()

    def write(self, buffer):
        if len(buffer) % self.SECTOR_SIZE:
            if not self.pad:
                raise NotImplementedError(f'Buffer Size has to be a multiple of 512 bytes, not {len(buffer)}')
            write_buffer = buffer.ljust(len(buffer) + (self.SECTOR_SIZE - (len(buffer) % self.SECTOR_SIZE)), b'\x00')
        else:
            write_buffer = buffer
        for i in range(len(write_buffer) // self.SECTOR_SIZE):
            result = self.fileobj.write(self._cipher.encrypt(write_buffer[self.SECTOR_SIZE * i: self.SECTOR_SIZE * (i + 1)]))
            self._next_sector()
        # flush the buffer explicitly to catch write errors earlier
        self.fileobj.flush()
        if self.sync:
            os.fsync(self.fileobj.fileno())

        self.bytes += len(buffer)
        return len(buffer)

    def close(self):
        self.fileobj.close()

    def tell(self):
        """
        This method is not entirely accurate in case a write operation has failed.
        :return: Number of bytes written. With the current implementation the amount of
        data written to disk and written to this file is identical except when padding is applied.
        """
        return self.bytes

    def _get_cipher(self):
        return AES.new(self.key, AES.MODE_CBC, IV=self.sector.to_bytes(16, byteorder='little'))


class AESTarFile:
    def __init__(self, file, passphrase, mode='wb', bufsize=131072, compression=None, sync=False):
        if mode != 'wb':
            raise NotImplementedError('Mode must be "wb"')
        self.aesfile = AESFile(passphrase=passphrase, file=file, mode=mode, bufsize=bufsize, sync=sync, pad=True)
        self.tarfile = tarfile.open(fileobj=self.aesfile, mode=f'w|{compression if compression else ""}',
                                    bufsize=bufsize)
        self.pending_files = []
        self.num_files = 0  # includes directories and special files
        self.previous_pending_length = 0

    def add(self, name, arcname=None):
        # always treat a file as pending after it has been added, therefore call purge first
        # this only makes a difference if you happen to exactly fill the buffer size with the new file
        # (and then cause a buffer flush)
        self.purge_pending()
        logging.debug(f'tarfile has {len(self.pending_files)} pending files, now adding {name}')
        try:
            self.tarfile.add(name, arcname=arcname, recursive=False)
        except OSError as e:
            self.purge_pending()
            if e.errno == errno.ENOSPC:
                # end of tape, try to close the file, causing a buffer flush and end of tape filemark to be written
                self.close_early()
            raise
        self.num_files += 1
        self.pending_files.append(
            (self.num_files, len(self.tarfile.fileobj.buf), self.aesfile.tell(), self.tarfile.offset))
        return self.pending_files[-1]

    def purge_pending(self):
        """
        Update self.pending_files to the current state. files that remain pending after the purge are not
        written to disk entirely.
        :return:
        """
        # i is the index in self.pending_files, stats[0] is the index (starting with 1)
        # of the file in all added files so far
        # stats[1] is the number of bytes currently in the (compressed) tarfile output buffer
        # stats[2] is the number of bytes written to disk with the aesfile
        # stats[3] is the tarfile byte offset (uncompressed)
        # both AFTER the file in question has been added
        # I am not 100% sure this will work correctly for all corner-cases of *compressed* tarfiles
        # because the tarfile.fileobj.buf is not filled when a very small file is added
        # and is instead buffered in the compressor buffer
        # tarfile.fileobj.cmp.flush(zlib.Z_FULL_FLUSH) might be needed to be safe.
        # TODO: this could probably be optimized by using two separate lists and calling .index() instead
        self.previous_pending_length = len(self.pending_files)
        remove_indices = [i for i, stats in enumerate(self.pending_files) if
                          (stats[1] + stats[2]) <= self.aesfile.tell()]

        if len(remove_indices) > 0:
            # all files up to and including the last index in remove_indices have been added
            # and can be removed from the pending list
            self.pending_files = self.pending_files[(remove_indices[-1] + 1):]

    @property
    def num_committed(self):
        return self.num_files - len(self.pending_files)

    def stats(self):
        return self.tarfile.offset, self.aesfile.tell()

    def close(self):
        # you could also call purge_pending twice because in theory writing the last 1024 zero bytes
        # as end of archive may fail, though all file contents have been written
        # self.purge_pending()
        self.tarfile.close()
        self.aesfile.close()
        self.purge_pending()

        if self.num_files != self.num_committed:
            raise Exception(f'Sanity check went wrong, archive has {self.num_files} files but {self.num_committed} commited.')

    def close_early(self):
        """
        This method only closes the (device) file descriptor, causing a buffer
        flush and end of tape file mark to be written.
        Calling close() instead will try to write the remaining tar buffer and AESFile buffer
        to the underlying device. In case it is full, this is (usually) not a good idea,
        unless tarfile would be extended to handle logical EOM retries (every other write fails with ENOSPC).
        """
        self.tarfile.closed = True
        self.tarfile.fileobj.closed = True
        self.aesfile.fileobj.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.close()
        else:
            # an uncaught exception occurred within the AESTarFile context
            # close the fileobject underlying the AESFile abstraction directly
            self.close_early()


class PendingQueue:
    def __init__(self, queue):
        self.queue = queue
        self.restore_queue = deque()
        self.restore = False

    def get(self):
        if self.restore:
            try:
                item = self.restore_queue.pop()
            except IndexError:
                logger.debug('Restore finished!')
                self.restore = False
                item = self.queue.get()
        else:
            item = self.queue.get()
        # we are intentionally feeding the item back into the restore queue even when restoring
        # it is removed by confirm() eventually
        self.restore_queue.appendleft(item)
        return item

    def confirm(self, num=1):
        return [self.restore_queue.pop() for _i in range(num)]

    def qsize(self):
        return self.queue.qsize() + len(self.restore_queue)

    def __len__(self):
        return self.qsize()
