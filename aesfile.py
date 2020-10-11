import hashlib
import os
from Crypto.Cipher import AES


class AESFile:
    def __init__(self, file, mode, passphrase, bufsize=512, sync=True, pad=True):
        """
        Python file object for transparent AES encryption compatible with `aespipe` in single-key mode.
        :param file: output file or device path to write to
        :param mode: the output file will be opened with mode
        :param passphrase: to derive the encryption key from. To be compatible with `aespipe` there may be no newline char at the end!
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
        if self.sync:
            # flush the buffer explicitly to catch write errors earlier
            self._file.flush()
            os.fsync(self._file.fileno())

        self.bytes += len(buffer)
        return len(buffer)

    def close(self):
        self._file.close()

    def tell(self):
        return self.bytes

    def tell_output(self):
        return self._file.tell()

    def _get_cipher(self):
        return AES.new(self.key, AES.MODE_CBC, IV=self.sector.to_bytes(16, byteorder='little'))
