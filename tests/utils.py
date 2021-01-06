import subprocess

def aespipe_decrypt(ciphertext_bytes, passphrase_file):
    aespipe = subprocess.Popen(['aespipe', '-d', '-P', passphrase_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    result = aespipe.communicate(input=ciphertext_bytes, timeout=10)
    return result[0]  # stdout


def tar_diff(tarfile, cwd=None):
    result = subprocess.run(['tar', '-dvf', tarfile], cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result


def untar(tarfile, directory=None):
    dir_change = ['-C', directory] if directory else []
    result = subprocess.run(['tar', '-xvf', tarfile] + dir_change, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result
