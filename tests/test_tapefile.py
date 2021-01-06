from aestar import aestar
from aestar import fakefile

import pytest


def test_fakefile_ok_write():
    f = fakefile.FakeFile(size=512)
    f.write(b'a'*511)
    f.close()


def test_fakefile_barely_ok_write():
    f = fakefile.FakeFile(size=512)
    f.write(b'a'*512)
    f.close()


def test_fakefile_fail_enospc():
    f = fakefile.FakeFile(size=512, failure_mode='ENOSPC')
    f.write(b'a'*256)
    with pytest.raises(IOError):
        f.write(b'a'*257)
    f.close()

def test_fakefile_write_return():
    f = fakefile.FakeFile(size=512, failure_mode='ENOSPC')
    result = f.write(b'a'*256)
    assert result == 256


def test_fakefile_fail_write0():
    f = fakefile.FakeFile(size=512, failure_mode='WRITE0')
    result = f.write(b'a'*513)
    assert result == 0
    f.close()


def test_tapefile():
    ff = fakefile.FakeFile(size=1024, failure_mode='WRITE0')
    with aestar.TapeFile(fileobj=ff) as f:
        f.write(b'a'*1000)
        with pytest.raises(IOError):
            f.write('b'*512)


def test_tapefile_file():
    f = aestar.TapeFile(file='tapefile.tmp')
    f.close()

def test_tapefile_both_file_and_fileobj():
    ff = fakefile.FakeFile()
    with pytest.raises(ValueError):
        with aestar.TapeFile(file='tapefile.tmp', fileobj=ff) as f:
            f.close()
