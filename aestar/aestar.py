import hashlib
import os
import tarfile
import warnings

from Crypto.Cipher import AES


class AESFile:
    def __init__(self, file, mode, passphrase, bufsize=512, sync=True, pad=True):
        """
        Python file object for transparent AES encryption compatible with `aespipe` in single-key mode.
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
            warnings.warn(f'Passphrase length incompatible with aespipe! {len(passphrase)}<20 characters.')
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
        self._file = open(file, mode=mode, buffering=self.bufsize)

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
            self._file.write(self._cipher.encrypt(write_buffer[self.SECTOR_SIZE * i: self.SECTOR_SIZE * (i + 1)]))
            self._next_sector()
        # flush the buffer explicitly to catch write errors earlier
        self._file.flush()
        if self.sync:
            os.fsync(self._file.fileno())

        self.bytes += len(buffer)
        return len(buffer)

    def close(self):
        self._file.close()

    def tell(self):
        return self.bytes

    def _get_cipher(self):
        return AES.new(self.key, AES.MODE_CBC, IV=self.sector.to_bytes(16, byteorder='little'))


class AESTarFile:
    def __init__(self, file, passphrase, mode='wb', bufsize=131072, compression=None, **kwargs):
        if mode != 'wb':
            raise NotImplementedError('Mode must be "wb"')
        self.aesfile = AESFile(file, mode=mode, passphrase=passphrase, bufsize=bufsize, sync=False, pad=True)
        self.tarfile = tarfile.open(fileobj=self.aesfile, mode=f'w|{compression if compression else ""}',
                                    bufsize=bufsize)
        self.pending_files = []
        self.num_files = 0  # includes directories and special files

    def add(self, name, arcname=None):
        # always treat a file as pending after it has been added, therefore call purge first
        # this only makes a difference if you happen to exactly fill the buffer size with the new file
        # (and then cause a buffer flush)
        self.purge_pending()
        print(f'{len(self.pending_files)} pending files, now adding {name}')
        self.tarfile.add(name, arcname=arcname, recursive=False)
        self.num_files += 1
        self.pending_files.append(
            (self.num_files, len(self.tarfile.fileobj.buf), self.aesfile.tell(), self.tarfile.offset))
        return self.pending_files[-1]

    def purge_pending(self):
        # i is the index in self.pending_files, stats[0] is the index of the file in all added files so far
        # stats[1] is the number of bytes currently in the (compressed) tarfile output buffer
        # stats[2] is the number of bytes written to disk with the aesfile
        # stats[3] is the tarfile byte offset (uncompressed)
        # both AFTER the file in question has been added
        # I am not 100% sure this will work correctly for all corner-cases of *compressed* tarfiles
        # because the tarfile.fileobj.buf is not filled when a very small file is added
        # and is therefore buffered in the e.g. gzip compressor buffer
        # a tarfile.fileobj.cmp.flush(zlib.Z_FULL_FLUSH) might be needed
        remove_indices = [i for i, stats in enumerate(self.pending_files) if
                          (stats[1] + stats[2]) <= self.aesfile.tell()]
        if len(remove_indices) > 0:
            # all files up to and including the last index in remove_indices have been added
            # and can be removed from the pending list
            self.pending_files = self.pending_files[remove_indices[-1] + 1:]

    def stats(self):
        return self.tarfile.offset, self.aesfile.tell()

    def close(self):
        # you could also call purge_pending twice because in theory writing the last 1024 zero bytes
        # at end of archive may fail, though all file contents have been written
        # self.purge_pending()
        self.tarfile.close()
        self.aesfile.close()
        self.purge_pending()
