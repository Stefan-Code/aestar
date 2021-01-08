from aestar import aestar
from aestar import fakefile
import time
import os
import pytest
import errno
from pathlib import Path
from .utils import aespipe_decrypt, untar, tar_diff, diff


@pytest.fixture(params=[r'passwords/ascii.txt', r'passwords/numeric.txt'])
def passphrase_file(request):
    return request.param


@pytest.fixture()
def passphrase(passphrase_file):
    with open(passphrase_file, 'rb') as f:
        return f.read().rstrip(b'\n')


def test_add_single_file(passphrase, tmp_path):
    f = aestar.AESTarFile(passphrase=passphrase, file=tmp_path / 'aestarfile.tar')
    assert f.num_files == 0
    f.add('./test_archive_folder/123.txt')
    assert f.num_files == 1
    f.close()


def test_context_manager(passphrase, tmp_path):
    with aestar.AESTarFile(passphrase, file=tmp_path / 'aestarfile.tar') as f:
        f.add('./test_archive_folder/lorem.txt')
    assert f.num_files == 1
    f.close()



def test_directory_tar_untar_diff(passphrase, passphrase_file, tmp_path):
    files = [p for p in Path('test_archive_folder').rglob('*')]
    # resulting file objects are relative paths
    with aestar.AESTarFile(passphrase=passphrase, file=tmp_path / 'aestarfile.tar.aes') as f:
        for file in files:
            f.add(file)
    assert f.num_files == len(files)
    f.close()
    with open(tmp_path / 'aestarfile.tar.aes', 'rb') as f:
        tar_data = aespipe_decrypt(f.read(), passphrase_file)
    with open(tmp_path / 'aestarfile.tar', 'wb') as f:
        f.write(tar_data)
    os.makedirs(tmp_path / 'extracted.temp.d')
    untar_result = untar(tmp_path / 'aestarfile.tar', tmp_path / 'extracted.temp.d')
    assert untar_result.returncode == 0
    diff_result = diff(tmp_path / 'extracted.temp.d/test_archive_folder/', 'test_archive_folder/')
    print(diff_result.stdout, diff_result.stderr)
    assert diff_result.returncode == 0


@pytest.mark.xfail(reason='mtime resolution is different between gnu tar and python os.stat()')
def test_directory_tar_diff(passphrase, passphrase_file, tmp_path):
    files = [p for p in Path('test_archive_folder').rglob('*')]
    # resulting file objects are relative paths
    with aestar.AESTarFile(passphrase=passphrase, file=tmp_path / 'aestarfile.tar.aes') as f:
        for file in files:
            f.add(file)
    assert f.num_files == len(files)
    f.close()
    with open(tmp_path / 'aestarfile.tar.aes', 'rb') as f:
        tar_data = aespipe_decrypt(f.read(), passphrase_file)
    with open(tmp_path / 'aestarfile.tar', 'wb') as f:
        f.write(tar_data)
    tar_diff_result = tar_diff(tmp_path / 'aestarfile.tar')
    print(tar_diff_result.stdout)
    assert len(tar_diff_result.stdout.rstrip().split(b'\n')) == len(files)
    assert tar_diff_result.returncode == 0


def test_EOT_close(passphrase):
    ff = fakefile.FakeFile(size=1024, failure_mode='ENOSPC')
    aestarfile = aestar.AESTarFile(passphrase=passphrase, fileobj=ff, bufsize=131072)
    aestarfile.add('test_archive_folder/random512')
    assert aestarfile.num_files == 1
    # this should not raise an error because we are still buffering up to 131072 bytes
    aestarfile.add('test_archive_folder/random1024')
    print(aestarfile.pending_files)
    with pytest.raises(IOError) as e_info:
        # adding a larger file should fail
        aestarfile.add('test_archive_folder/random10MB')
    assert e_info.value.errno == errno.ENOSPC
    assert aestarfile.closed is True


def test_num_committed(passphrase):
    ff = fakefile.FakeFile(size=-1, failure_mode='ENOSPC')
    aestarfile = aestar.AESTarFile(passphrase=passphrase, fileobj=ff, bufsize=512)
    aestarfile.add('test_archive_folder/random512')
    assert aestarfile.num_files == 1
    assert aestarfile.num_committed == 0
    aestarfile.add('test_archive_folder/random2048')
    aestarfile.add('test_archive_folder/random512')
    assert aestarfile.num_committed == 1
    aestarfile.purge_pending()
    assert aestarfile.num_committed == 2
    aestarfile.close()
    assert aestarfile.num_committed == 3

