import subprocess

import pytest

from aestar import aestar
from .utils import aespipe_decrypt

# These tests require the program `aespipe` to be available on your system


@pytest.fixture(params=[r'passwords/ascii.txt', r'passwords/numeric.txt'])
def passphrase_file(request):
    return request.param


@pytest.fixture()
def passphrase(passphrase_file):
    with open(passphrase_file, 'rb') as f:
        return f.read().rstrip(b'\n')


@pytest.fixture()
def short_passphrase():
    with open(f'passwords/short.txt', 'rb') as f:
        return f.read().rstrip(b'\n')


@pytest.fixture(params=['random512', 'random1024', 'random2048', 'random10240'])
def plaintext_file(request):
    return f'test_archive_folder/{request.param}'


@pytest.fixture()
def plaintext(plaintext_file):
    with open(plaintext_file, 'rb') as f:
        return f.read()


@pytest.fixture()
def aesfile(passphrase, tmp_path):
    return aestar.AESFile(passphrase=passphrase, file=tmp_path / 'aesfile.tmp')



def test_short_password_warning(short_passphrase, tmp_path):
    with pytest.warns(Warning):
        aesfile = aestar.AESFile(short_passphrase, file=tmp_path / 'aesfile.tmp')


def test_encrypt_decrypt_aespipe(aesfile, passphrase_file, plaintext, tmp_path):
    aesfile.write(plaintext)
    aesfile.close()
    with open(tmp_path / 'aesfile.tmp', 'rb') as f:
        ciphertext_bytes = f.read()
    decrypted = aespipe_decrypt(ciphertext_bytes, passphrase_file)
    assert plaintext == decrypted


def test_fileobj(passphrase, tmp_path):
    with open(tmp_path / 'aesfile_obj.tmp', 'wb') as f:
        aesfile = aestar.AESFile(passphrase, fileobj=f)
        aesfile.write(b'123')
        aesfile.close()


def test_both_file_and_fileobj(tmp_path):
    with pytest.raises(ValueError):
        with open(tmp_path / 'aesfile2.tmp', 'wb') as f:
            aestar.AESFile(passphrase=b'12345678901234567890', file=tmp_path / 'aesfile.tmp', fileobj=f)


def test_missing_file_argument():
    with pytest.raises(ValueError):
        aestar.AESFile(passphrase=b'12345678901234567890')

