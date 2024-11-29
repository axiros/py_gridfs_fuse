from setuptools import setup
from setuptools import find_packages

setup(
    name="gridfs_fuse",
    version="0.3.1",
    install_requires=[
        'llfuse>=1.5.0',
        'pymongo',
    ],
    include_package_data=True,
    package_dir={'gridfs_fuse': 'gridfs_fuse'},
    packages=find_packages('.'),
    entry_points={
        'console_scripts': [
            'gridfs_fuse = gridfs_fuse.main:main',
        ]
    }
)
