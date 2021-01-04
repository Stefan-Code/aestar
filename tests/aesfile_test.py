import subprocess

import pytest

from pathlib import Path

from aestar import aestar



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
def aesfile(passphrase):
    return aestar.AESFile(passphrase=passphrase, file='aesfile.tmp')


def aespipe_decrypt(ciphertext_bytes, passphrase_file):
    aespipe = subprocess.Popen(['aespipe', '-d', '-P', passphrase_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    result = aespipe.communicate(input=ciphertext_bytes, timeout=10)
    return result[0]  # stdout


def test_short_password_warning(short_passphrase):
    with pytest.warns(Warning):
        aesfile = aestar.AESFile(short_passphrase, file='aesfile.tmp')

def test_encrypt_decrypt_aespipe(aesfile, passphrase_file, plaintext):
    aesfile.write(plaintext)
    aesfile.close()
    with open('aesfile.tmp', 'rb') as f:
        ciphertext_bytes = f.read()
    decrypted = aespipe_decrypt(ciphertext_bytes, passphrase_file)
    assert plaintext == decrypted

def test_fileobj(passphrase):
    with open('aesfile_obj.tmp', 'wb') as f:
        aesfile = aestar.AESFile(passphrase, fileobj=f)
        aesfile.write(b'123')
        aesfile.close()

def test_both_file_and_fileobj():
    with pytest.raises(ValueError):
        with open('aesfile2.tmp', 'wb') as f:
            aestar.AESFile(passphrase=b'12345678901234567890', file='aesfile.tmp', fileobj=f)

def test_missing_file_argument():
    with pytest.raises(ValueError):
        aestar.AESFile(passphrase=b'12345678901234567890')


def a_test_aesfile_data_aespipe(aesfile, passphrase, plaintext, aesfile_name):
    aesfile.write(plaintext)
    aesfile.close()
    with open('pw.txt', 'wb') as f:
        f.write(passphrase)
    with open('plaintext.txt', 'wb') as f:
        f.write(plaintext)
    with open(aesfile_name, 'rb') as f:
        encrypted_data = f.read()
    aespipe = subprocess.Popen(['aespipe', '-d', '-P', 'pw.txt'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    result = aespipe.communicate(input=encrypted_data, timeout=10)

    assert result[0] == plaintext

# aestar.AESFile
# cwd to tmp dir
# test with deterministic and random data (fuzzing)
