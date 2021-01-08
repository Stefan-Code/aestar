import errno
import os


class FakeFile:
    def __init__(self, size=-1, failure_mode='ENOSPC'):
        if not failure_mode in ('ENOSPC', 'WRITE0'):
            raise ValueError(f'Unknown failure mode "{failure_mode}"')
        self.buffer = []
        self.closed = False
        self.size = size
        self.written = 0
        self.failure_mode = failure_mode

    def close(self):
        self.buffer = []
        self.closed = True

    def write(self, buffer):
        if self.closed:
            raise ValueError('FakeFile already closed')
        if self.size >= 0 and len(buffer) + self.written > self.size:
            # we are certain this write is going to cause an error
            if self.failure_mode == 'ENOSPC':
                raise IOError(errno.ENOSPC, os.strerror(errno.ENOSPC))
            elif self.failure_mode == 'WRITE0':
                return 0
        else:
            # normal write operation, returns length of written bytes
            self.buffer.append(buffer)
            self.written += len(buffer)
            return len(buffer)

    def flush(self):
        pass
