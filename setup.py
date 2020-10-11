try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
setup(
  name = 'aestar',
  packages = ['aestar'],
  version = '0.0.1',
  license='GPLv3+',
  description = 'Write AES encrypted tar backups to LTO tapes or files',
  author = 'Stefan Kuntz',
  author_email = 'Stefan.github@gmail.com',
  url = 'https://github.com/Stefan-Code/aestar',
  keywords = ['tar', 'archive', 'encryption', 'aes', 'tape', 'LTO'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'sqlite3',
          'pycryptodome',
          'click',
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: '
    'GNU General Public License v3 or later (GPLv3+)',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)
