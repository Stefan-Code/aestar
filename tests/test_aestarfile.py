from aestar import aestar
import time
import os
import pytest
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
    f = aestar.AESTarFile(tmp_path / 'aestarfile.tar', passphrase)
    assert f.num_files == 0
    f.add('./test_archive_folder/123.txt')
    assert f.num_files == 1
    f.close()


def test_context_manager(passphrase, tmp_path):
    with aestar.AESTarFile(tmp_path / 'aestarfile.tar', passphrase) as f:
        f.add('./test_archive_folder/lorem.txt')
    assert f.num_files == 1
    f.close()



def test_directory_tar_untar_diff(passphrase, passphrase_file, tmp_path):
    files = [p for p in Path('test_archive_folder').rglob('*')]
    # resulting file objects are relative paths
    with aestar.AESTarFile(tmp_path / 'aestarfile.tar.aes', passphrase) as f:
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
    with aestar.AESTarFile(tmp_path / 'aestarfile.tar.aes', passphrase) as f:
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

