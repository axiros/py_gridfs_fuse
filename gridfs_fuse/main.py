import argparse
import logging
import llfuse

from bson import Binary
from gridfs_fuse.operations import operations_factory
from gridfs_fuse.operations import create_mongo_client
from gridfs_fuse.pymongo_compat import compat_collection
from gridfs_fuse.version import __version__


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


def perform_startup_migrations(database):
    meta_col = compat_collection(database, 'meta')
    metadata_col = compat_collection(database, 'metadata')
    fs_files_col = compat_collection(database, 'fs.files')

    version_doc = meta_col.find_one({"_id": "version"})
    version = version_doc["value"] if version_doc else "0.0.0"

    if version < "0.3.0":
        for col in [fs_files_col, metadata_col]:
            for doc in col.find({}):
                update_fields = {}
                unset_fields = {}
                # Filename conversion
                if "filename" in doc and isinstance(doc["filename"], str):
                    update_fields["filename"] = Binary(doc["filename"].encode())

                # Timestamp fields migration
                for ts_field in ['atime', 'mtime', 'ctime']:
                    if ts_field in doc:
                        update_fields[f"{ts_field}_ns"] = int(doc[ts_field] * 1e6)
                        unset_fields[ts_field] = ""

                if update_fields:
                    col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": update_fields, "$unset": unset_fields}
                    )

        meta_col.update_one(
            {"_id": "version"},
            {"$set": {"value": __version__}},
            upsert=True
        )


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
