from importlib import metadata
from distutils.version import LooseVersion

from bson import Binary
from gridfs_fuse.pymongo_compat import compat_collection


MAX_MIGRATION_VERSION = LooseVersion("0.3.0")


def perform_startup_migrations(database):
    """Perform database migration due to new llfuse version.

    Example usage:
        from gridfs_fuse.operations import create_mongo_client

        client = create_mongo_client("mongodb://127.0.0.1:27017")
        db = client["gridfs_fuse"]
        perform_startup_migrations(db)
    """

    meta_col = compat_collection(database, 'meta')
    metadata_col = compat_collection(database, 'metadata')
    fs_files_col = compat_collection(database, 'fs.files')

    version_doc = meta_col.find_one({"_id": "version"})
    version = version_doc["value"] if version_doc else "0.0.0"
    version = LooseVersion(version)

    if version < MAX_MIGRATION_VERSION:
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
            {"$set": {"value": metadata.version("gridfs_fuse")}},
            upsert=True
        )
