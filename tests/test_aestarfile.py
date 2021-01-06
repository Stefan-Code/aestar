from aestar import aestar
import time

import pytest
from pathlib import Path
from .utils import aespipe_decrypt, untar, tar_diff


@pytest.fixture(params=[r'passwords/ascii.txt', r'passwords/numeric.txt'])
def passphrase_file(request):
    return request.param


@pytest.fixture()
def passphrase(passphrase_file):
    with open(passphrase_file, 'rb') as f:
        return f.read().rstrip(b'\n')


def test_add_single_file(passphrase):
    f = aestar.AESTarFile('aestarfile.tar', passphrase)
    assert f.num_files == 0
    f.add('./test_archive_folder/123.txt')
    assert f.num_files == 1
    f.close()


def test_context_manager(passphrase):
    with aestar.AESTarFile('aestarfile.tar', passphrase) as f:
        f.add('./test_archive_folder/lorem.txt')
    assert f.num_files == 1
    f.close()



def test_directory_tar_untar(passphrase, passphrase_file):
    files = [p for p in Path('test_archive_folder').rglob('*')]
    # resulting file objects are relative paths
    with aestar.AESTarFile('aestarfile.tar.aes', passphrase) as f:
        for file in files:
            f.add(file)
    assert f.num_files == len(files)
    f.close()
    with open('aestarfile.tar.aes', 'rb') as f:
        tar_data = aespipe_decrypt(f.read(), passphrase_file)
    with open('aestarfile.tar', 'wb') as f:
        f.write(tar_data)
    tar_diff_result = tar_diff('aestarfile.tar')
    assert len(tar_diff_result.stdout.rstrip().split(b'\n')) == len(files)
    assert tar_diff_result.returncode == 0

