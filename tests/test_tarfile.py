import tarfile
from pathlib import Path
import os


def test_metadata(tmp_path):
    # this python-only test should pass
    # however, os.stat() may have a lower resolution of mtime than the system stat
    # therefore a gnu tar diff fails with exit code 1
    files = [p for p in Path('test_archive_folder').rglob('*')]
    f = tarfile.TarFile(tmp_path / 'aestarfile.tar', 'w')
    for file in files:
        f.add(file)
        assert f.members[-1].mtime == os.stat(file).st_mtime

