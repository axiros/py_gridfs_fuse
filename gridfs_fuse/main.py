import argparse
import logging
import llfuse

from gridfs_fuse.operations import operations_factory
from gridfs_fuse.operations import create_mongo_client
from gridfs_fuse.migrations import perform_startup_migrations


def configure_argparse(parser):
    parser.add_argument(
        '--mongodb-uri',
        dest='mongodb_uri',
        default="mongodb://127.0.0.1:27017",
        help="Connection string for MongoClient. http://goo.gl/abqY9",
        required=True)

    parser.add_argument(
        '--database',
        dest='database',
        default='gridfs_fuse',
        help="Name of the database where the filesystem goes",
        required=True)

    parser.add_argument(
        '--mount-point',
        dest='mount_point',
        help="Path where to mount fuse/gridfs wrapper",
        required=True)

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default="INFO",
        help="Set the logging level")

    return parser


def run_fuse_mount(ops, options, mount_opts):
    mount_opts = ['fsname=gridfs_fuse'] + mount_opts
    llfuse.init(ops, options.mount_point, mount_opts)

    try:
        llfuse.main(workers=1)
    finally:
        llfuse.close()


def main():
    parser = argparse.ArgumentParser()
    configure_argparse(parser)
    options = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=getattr(logging, options.log_level.upper()))

    ops = operations_factory(options)

    client = create_mongo_client(options.mongodb_uri)
    db = client[options.database]
    perform_startup_migrations(db)

    # TODO: Still not sure which options to use
    # 'allow_other' Regardless who mounts it, all other users can access it
    # 'default_permissions' Let the kernel do the permission checks
    # 'nonempty' Allow mount on non empty directory
    mount_opts = ['default_permissions']

    run_fuse_mount(ops, options, mount_opts)


if __name__ == '__main__':
    main()
