from setuptools import setup
from setuptools import find_packages
import os
import sys

here = os.path.abspath(os.path.dirname(__file__)) + '/'

mount_script = 'mount.gridfs' if sys.platform != 'darwin' else 'mount_gridfs'

setup(
    name="gridfs_fuse",
    url='https://github.com/axiros/py_gridfs_fuse',
    description=open(here + 'README.md').readlines()[1].strip('\n'),
    license=open(here + 'LICENSE.md').readlines()[0].strip('\n'),
    version=open(here + 'VERSION').read().strip('\n') or '0.1.1',
    install_requires=open(here + 'requirements.txt').readlines(),
    include_package_data=True,
    package_dir={'gridfs_fuse': 'gridfs_fuse'},
    packages=find_packages('.'),
    entry_points={
        'console_scripts': [
            'gridfs_fuse = gridfs_fuse.main:main',
            '%s = gridfs_fuse.main:_mount_fuse_main' %(mount_script),
        ]
    }
)
