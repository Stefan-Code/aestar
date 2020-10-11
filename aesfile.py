import hashlib
from Crypto.Cipher import AES


class AESFile:
    def __init__(self, file, mode, passphrase, bufsize=512):
        self.SECTOR_SIZE = 512  # bytes
        if bufsize < self.SECTOR_SIZE:
            raise ValueError(f'Buffer Size has to be at least {self.SECTOR_SIZE} bytes, not {bufsize}')
        elif bufsize % self.SECTOR_SIZE:
            raise ValueError(f'Buffer Size has to be a multiple of {self.SECTOR_SIZE} bytes, not {bufsize}')
        self.bufsize = bufsize
        # actual file we are writing to
        # turn buffering off, as we are writing buffered chunks anyways
        self._file = open(file, mode=mode, buffering=self.bufsize)
        # the encryption key is derived from the upper 16 bytes of the SHA256 hash (in case of AES128)
        self.key = hashlib.sha256(passphrase).digest()[:16]
        # aespipe in single-key mode uses a 0-byte IV which is incremented for each 512 byte sector
        self.sector = 0
        self._cipher = self._get_cipher()
        self._encrypted_buffer = bytes(self.bufsize)

    def _next_sector(self):
        self.sector += 1
        self._cipher = self._get_cipher()

    def write(self, buffer):
        if len(buffer) % self.SECTOR_SIZE:
            raise NotImplementedError(f'Buffer Size has to be a multiple of 512 bytes, not {len(buffer)}')
        for i in range(len(buffer) // self.SECTOR_SIZE):
            self._file.write(self._cipher.encrypt(buffer[self.SECTOR_SIZE * i: self.SECTOR_SIZE * (i + 1)]))
            self._next_sector()
        # flush the buffer explicitly to catch write errors earlier
        self._file.flush()
        return len(buffer)

    def close(self):
        self._file.close()

    def _get_cipher(self):
        return AES.new(self.key, AES.MODE_CBC, IV=self.sector.to_bytes(16, byteorder='little'))
