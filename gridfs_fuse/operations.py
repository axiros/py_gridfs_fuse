import logging
import os
import stat
import time
import errno
import collections
import threading

import llfuse
import gridfs

import pymongo
from .pymongo_compat import compat_collection

from distutils.version import LooseVersion


mask = stat.S_IWGRP | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH


RETRY_WRITES_MIN_VERSION = LooseVersion("3.6")


def grid_in_size(grid_in):
    return grid_in._position + grid_in._buffer.tell()


class EntryNotFound(Exception):
    @classmethod
    def make(cls, inode):
        return cls("Could not find record in mongo for inode: %s" % inode)


class Entry(object):
    def __init__(self, ops, filename, inode, parent_inode, mode, uid, gid):
        self._ops = ops

        self._id = inode
        self.filename = filename
        self.parent_inode = parent_inode
        self.mode = mode
        self.uid = uid
        self.gid = gid

        self.atime = self.mtime = self.ctime = int(time.time_ns())

        # Only for directories
        # filename: inode
        self.childs = {}

    @property
    def inode(self):
        return self._id


class FileDescriptorFactory(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.next_fd = 1
        self.active_fd = set()

    def gen(self):
        with self.lock:
            while self.next_fd in self.active_fd:
                self.next_fd = (self.next_fd + 1) % (2 ** 16)

            self.active_fd.add(self.next_fd)
            return self.next_fd

    def release(self, fd):
        with self.lock:
            self.active_fd.discard(fd)

            if not self.active_fd:
                self.next_fd = 1


class Operations(llfuse.Operations):
    def __init__(self, database):
        super(Operations, self).__init__()

        self.logger = logging.getLogger("gridfs_fuse")

        self.meta = compat_collection(database, 'metadata')
        self.gridfs = gridfs.GridFS(database)
        self.gridfs_files = compat_collection(database, 'fs.files')

        # For syscalls which return a 'file handle'.
        # This handle is used later to associate successive calls.
        self.fd_factory = FileDescriptorFactory()

        # Mapping between fd: GridFile
        self.active_writes = {}
        self.active_reads = {}

    def open(self, inode, flags, ctx):
        self.logger.debug("open: %s %s", inode, flags)

        # Do not allow writes to a existing file
        if flags & os.O_WRONLY:
            raise llfuse.FUSEError(errno.EACCES)

        try:
            reader = self.gridfs.get(inode)
        except gridfs.errors.NoFile:
            msg = "Read of inode (%s) fails. Gridfs object not found"
            self.logger.error(msg, inode)
            raise llfuse.FUSEError(errno.EIO)

        fd = self.fd_factory.gen()
        self.active_reads[fd] = reader

        return fd

    def opendir(self, inode, ctx):
        """Just to check access, dont care about access => return inode"""
        self.logger.debug("opendir: %s", inode)
        return inode

    def access(self, inode, mode, ctx):
        """Again this fs does not care about access"""
        self.logger.debug("access: %s %s %s", inode, mode, ctx)
        return True

    def getattr(self, inode, ctx):
        self.logger.debug("getattr: %s", inode)
        return self._gen_attr(self._entry_by_inode(inode))

    def readdir(self, inode, off):
        self.logger.debug("readdir: %s %s", inode, off)

        entry = self._entry_by_inode(inode)

        for child_inode in sorted(entry.childs.values()):
            if child_inode <= off:
                continue

            try:
                child = self._entry_by_inode(child_inode)
            except EntryNotFound:
                # Looks like entry got deleted while iterating over the folder
                continue

            item = (child.filename, self._gen_attr(child), child_inode)
            yield item

    def lookup(self, folder_inode, name, ctx):
        self.logger.debug("lookup: %s %s", folder_inode, name)

        if name == '.':
            inode = folder_inode

        elif name == '..':
            entry = self._entry_by_inode(folder_inode)
            inode = entry.parent_inode

        else:
            entry = self._entry_by_inode(folder_inode)
            for fn, inode in entry.childs.items():
                if fn == name:
                    break
            else:
                raise llfuse.FUSEError(errno.ENOENT)

        return self.getattr(inode, ctx)

    def mknod(self, inode_p, name, mode, rdev, ctx):
        self.logger.debug("mknod")
        raise llfuse.FUSEError(errno.ENOSYS)

    def mkdir(self, folder_inode, name, mode, ctx):
        self.logger.debug("mkdir: %s %s %s %s", folder_inode, name, mode, ctx)
        entry = self._create_entry(folder_inode, name, mode, ctx)
        return self._gen_attr(entry)

    def create(self, folder_inode, name, mode, flags, ctx):
        self.logger.debug("create: %s %s %s %s", folder_inode, name, mode, flags)

        entry = self._create_entry(folder_inode, name, mode, ctx)

        fd = self.fd_factory.gen()
        self.active_writes[fd] = self._create_grid_in(entry)

        return (fd, self._gen_attr(entry))

    def _create_grid_in(self, entry):
        gridfs_filename = self._create_full_path(entry)
        return self.gridfs.new_file(_id=entry.inode, filename=gridfs_filename)

    def _encode_if_str(self, str_or_bytes):
        if isinstance(str_or_bytes, str):
            return str_or_bytes.encode()
        return str_or_bytes

    def _create_full_path(self, entry):
        # Build the full path for this file.
        # Add the full path to make other tools like
        # mongofiles, mod_gridfs, ngx_gridfs happy
        path = collections.deque()
        while entry._id != llfuse.ROOT_INODE:
            path.appendleft(self._encode_if_str(entry.filename))
            entry = self._entry_by_inode(entry.parent_inode)
        path.appendleft(self._encode_if_str(entry.filename))
        return os.path.join(*path)

    def _create_entry(self, folder_inode, name, mode, ctx):
        inode = self._gen_inode()
        entry = Entry(self, name, inode, folder_inode, mode, ctx.uid, ctx.gid)

        self._insert_entry(entry)

        query = {"_id":  folder_inode}
        update = {"$addToSet": {"childs": (name, inode)}}
        self.meta.update_one(query, update)

        return entry

    def setattr(self, inode, attr, fields, fh, ctx):
        self.logger.debug("setattr: %s %s", inode, attr)

        entry = self._entry_by_inode(inode)

        # Now way to change the size of an existing file.
        if attr.st_size is not None:
            raise llfuse.FUSEError(errno.EINVAL)

        if attr.st_rdev is not None:
            raise llfuse.FUSEError(errno.ENOSYS)

        to_set = [
            'st_mode',
            'st_uid',
            'st_gid',
            'st_atime',
            'st_mtime',
            'st_ctime'
        ]

        for attr_name in to_set:
            val = getattr(attr, attr_name, None)
            if val is not None:
                target = attr_name[3:]
                setattr(entry, target, val)

        self._update_entry(entry)
        return self._gen_attr(entry)

    def unlink(self, folder_inode, name, ctx):
        self.logger.debug("unlink: %s %s", folder_inode, name)

        self._delete_inode(
            folder_inode,
            name,
            self._delete_inode_check_file)

    def rmdir(self, folder_inode, name, ctx):
        self.logger.debug("rmdir: %s %s", folder_inode, name)

        self._delete_inode(
            folder_inode,
            name,
            self._delete_inode_check_directory)

    def _delete_inode(self, folder_inode, name, entry_check):
        # On insert the order is like this
        # 1. write into the database.
        #    the unique index (parent_inode, filename) protects
        # 2. Update the folder inode

        # On remove the order must be vice verca
        # 1. Remove from the folder inode
        # 2. Remove from the database

        # In that case the unique index protection is true

        parent = self._entry_by_inode(folder_inode)

        if name not in parent.childs:
            raise llfuse.FUSEError(errno.ENOENT)
        inode = parent.childs[name]

        entry = self._entry_by_inode(inode)
        entry_check(entry)

        # Remove from the folder node
        query = {"_id": folder_inode}
        update = {"$pull": {'childs': (name, inode)}}
        self.meta.update_one(query, update)

        # Remove from the database
        self.meta.delete_one({"_id": inode})

        # Remove from the grids collections
        self.gridfs.delete(inode)

    def _delete_inode_check_file(self, entry):
        if stat.S_ISDIR(entry.mode):
            raise llfuse.FUSEError(errno.EISDIR)

    def _delete_inode_check_directory(self, entry):
        if not stat.S_ISDIR(entry.mode):
            raise llfuse.FUSEError(errno.ENOTDIR)

        if len(entry.childs) > 0:
            raise llfuse.FUSEError(errno.ENOTEMPTY)

    def read(self, fd, offset, length):
        self.logger.debug("read: %s %s %s", fd, offset, length)

        if fd not in self.active_reads:
            self.logger.error("wrong fd on read: %s %s %s", fd, offset, length)
            raise llfuse.FUSEError(errno.EINVAL)

        grid_out = self.active_reads[fd]

        grid_out.seek(offset)
        return grid_out.read(length)

    def write(self, fd, offset, data):
        # Only 'append once' semantics are supported.
        self.logger.debug("write: %s %s %s", fd, offset, len(data))

        if fd not in self.active_writes:
            self.logger.error("wrong fd on write: %s %s %s", fd, offset, len(data))
            raise llfuse.FUSEError(errno.EINVAL)

        grid_in = self.active_writes[fd]

        if offset != grid_in_size(grid_in):
            raise llfuse.FUSEError(errno.EINVAL)

        grid_in.write(data)
        return len(data)

    def release(self, fd):
        self.logger.debug("release: %s", fd)

        if fd in self.active_writes:
            self.active_writes.pop(fd).close()

        if fd in self.active_reads:
            self.active_reads.pop(fd).close()

        self.fd_factory.release(fd)

    def releasedir(self, inode):
        self.logger.debug("releasedir: %s", inode)

    def forget(self, inode_list):
        self.logger.debug("forget: %s", inode_list)

    def readlink(self, inode, ctx):
        self.logger.debug("readlink: %s", inode)
        raise llfuse.FUSEError(errno.ENOSYS)

    def symlink(self, folder_inode, name, target, ctx):
        self.logger.debug("symlink: %s %s %s", folder_inode, name, target)
        raise llfuse.FUSEError(errno.ENOSYS)

    def rename(self, old_folder_inode, old_name, new_folder_inode, new_name, ctx):
        self.logger.debug(
            "rename: %s %s %s %s",
            old_folder_inode,
            old_name,
            new_folder_inode,
            new_name)

        # Load the entry to move
        entry_attributes = self.lookup(old_folder_inode, old_name)
        entry = self._entry_by_inode(entry_attributes.st_ino)

        # Load the target directory
        new_folder = self._entry_by_inode(new_folder_inode)

        # Check if the folder already contains this name and remove it.
        if new_name in new_folder.childs:
            noop = lambda entry: None
            self._delete_inode(new_folder.inode, new_name, noop)

        # Set the new parent and filename to the existing inode.
        query = {"_id": entry.inode}
        update = {
            "$set": {
                'parent_inode': new_folder.inode,
                'filename': new_name
            }
        }
        self.meta.update_one(query, update)

        # Erase the inode from the older folder.
        query = {"_id": old_folder_inode}
        update = {"$pull": {'childs': (entry.filename, entry.inode)}}
        self.meta.update_one(query, update)

        # Add the inode to the new folder
        query = {"_id": new_folder.inode}
        update = {"$addToSet": {'childs': (new_name, entry.inode)}}
        self.meta.update_one(query, update)

        # Ensure the correct filename within gridfs
        entry.parent_inode = new_folder.inode
        entry.filename = new_name

        gridfs_filename = self._create_full_path(entry)
        query = {"_id": entry.inode}
        update = {"$set": {'filename': gridfs_filename}}
        self.gridfs_files.update_one(query, update)

    def link(self, inode, new_parent_inode, new_name, ctx):
        self.logger.debug("link: %s %s %s", inode, new_parent_inode, new_name)
        raise llfuse.FUSEError(errno.ENOSYS)

    def flush(self, fd):
        self.logger.debug("flush: %s", fd)
        raise llfuse.FUSEError(errno.ENOSYS)

    def fsync(self, fd, datasync):
        self.logger.debug("fsync: %s %s", fd, datasync)
        raise llfuse.FUSEError(errno.ENOSYS)

    def fsyncdir(self, fd, datasync):
        self.logger.debug("fsyncdir: %s %s", fd, datasync)
        raise llfuse.FUSEError(errno.ENOSYS)

    def statfs(self, ctx):
        self.logger.debug("statfs")
        raise llfuse.FUSEError(errno.ENOSYS)

    def _entry_by_inode(self, inode):
        query = {'_id': inode}

        record = self.meta.find_one(query)
        if record is None:
            raise EntryNotFound.make(inode)

        return self._doc_to_entry(record)

    def _insert_entry(self, entry):
        doc = self._entry_to_doc(entry)
        self.meta.insert_one(doc)

    def _update_entry(self, entry):
        query = {"_id": entry.inode}
        doc = self._entry_to_doc(entry)
        self.meta.update_one(query, {"$set": doc})

    def _entry_to_doc(self, entry):
        doc = dict(vars(entry))
        del doc['_ops']
        doc['childs'] = list(entry.childs.items())
        return doc

    def _doc_to_entry(self, doc):
        doc['_ops'] = self
        doc['childs'] = dict(doc['childs'])
        entry = object.__new__(Entry)
        entry.__dict__.update(doc)
        return entry

    def _gen_attr(self, entry):
        attr = llfuse.EntryAttributes()

        attr.st_ino = entry.inode
        attr.generation = 0
        attr.entry_timeout = 10
        attr.attr_timeout = 10

        attr.st_mode = entry.mode
        attr.st_nlink = 1

        attr.st_uid = entry.uid
        attr.st_gid = entry.gid
        attr.st_rdev = 0

        attr.st_size = self._get_entry_size(entry)

        attr.st_blksize = 512
        attr.st_blocks = (attr.st_size // attr.st_blksize) + 1

        attr.st_atime_ns = int(entry.atime)
        attr.st_mtime_ns = int(entry.mtime)
        attr.st_ctime_ns = int(entry.ctime)

        return attr

    def _get_entry_size(self, entry):
        if stat.S_ISDIR(entry.mode):
            return 4096

        if entry.inode in self.active_writes:
            return grid_in_size(self.active_writes[entry.inode])

        # pymongo creates the entry only when the file is completely written
        # and *closed* by the writer.
        # => As long as the file is written (not closed) 'self.gridfs.get'
        # returns an ERROR on other nodes doing a 'get'.
        # This happens on other nodes *not* doing the actual write.
        # The node doing the write has the current file-object in memory
        # (self.active_writes).
        # => As long as the file is written, other nodes see only size=0
        try:
            return self.gridfs.get(entry._id).length
        except gridfs.errors.NoFile:
            return 0

    def _gen_inode(self):
        query = {"_id": "next_inode"}
        update = {"$inc": {"value": 1}}
        doc = self.meta.find_one_and_update(query, update)
        return doc['value']


def _ensure_root_inode(ops):
    root = Entry(
        ops,
        '/',
        llfuse.ROOT_INODE,
        llfuse.ROOT_INODE,
        stat.S_IFDIR | stat.S_IRWXU | mask,
        os.getuid(),
        os.getgid())

    try:
        ops._insert_entry(root)
    except pymongo.errors.DuplicateKeyError:
        pass


def _ensure_next_inode_document(ops):
    # Use this document to create inodes
    try:
        ops.meta.insert_one({
            "_id": "next_inode",
            'value': llfuse.ROOT_INODE + 1
        })
    except pymongo.errors.DuplicateKeyError:
        pass


def _ensure_indexes(ops):
    # Use this index to ensure that now 'duplicate' documents
    # are in the same folder
    index = [
        ('parent_inode', pymongo.ASCENDING),
        ('filename', pymongo.ASCENDING)
    ]
    ops.meta.create_index(index, unique=True)


def get_compat_version(client):
    compat_cmd = {"getParameter": 1, "featureCompatibilityVersion": 1}
    cmd_response = client.admin.command(compat_cmd)
    compat_version = cmd_response["featureCompatibilityVersion"]
    if "version" in compat_version:
        compat_version = compat_version["version"]

    return LooseVersion(compat_version)


def operations_factory(options):
    logger = logging.getLogger("gridfs_fuse")

    old_pymongo = LooseVersion(pymongo.version) < LooseVersion("3.6.0")

    if old_pymongo:
        client = pymongo.MongoClient(options.mongodb_uri)
    else:
        client = pymongo.MongoClient(options.mongodb_uri, retryWrites=True)

    compat_version = get_compat_version(client)
    if old_pymongo or compat_version < RETRY_WRITES_MIN_VERSION:
        logger.warning(
                "Your featureCompatibilityVersion (%s) is lower than the "
                "required %s for retryable writes to work. "
                "Due to this file operations might fail if failovers happen."
                "Additionally, this feature requires pymongo >= 3.6.0 "
                "(Yours: %s).",
                compat_version,
                RETRY_WRITES_MIN_VERSION,
                pymongo.version)

    ops = Operations(client[options.database])
    _ensure_root_inode(ops)
    _ensure_next_inode_document(ops)
    _ensure_indexes(ops)

    return ops
