import logging
import os
import stat
import time
import errno
import collections

import llfuse
import gridfs

import pymongo
from .pymongo_compat import compat_collection


mask = stat.S_IWGRP | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH


def grid_in_size(grid_in):
    return grid_in._position + grid_in._buffer.tell()


class Entry(object):
    def __init__(self, ops, filename, inode, parent_inode, mode, uid, gid):
        self._ops = ops

        self._id = inode
        self.filename = filename
        self.parent_inode = parent_inode
        self.mode = mode
        self.uid = uid
        self.gid = gid

        self.atime = self.mtime = self.ctime = int(time.time())

        # Only for directories
        # filename: inode
        self.childs = {}

    @property
    def inode(self):
        return self._id


class Operations(llfuse.Operations):
    def __init__(self, database, collection='fs', logfile=None, debug=os.environ.get('GRIDFS_FUSE_DEBUG')):
        super(Operations, self).__init__()

        self.logger = logging.getLogger("gridfs_fuse")
        self.logger.setLevel(logging.DEBUG if debug else logging.ERROR)
        try:
            self.handler = logging.FileHandler(logfile)
            self.handler.setLevel(logging.DEBUG)
        except:
            pass
        #self._readonly = read_only
        self._database = database
        self._collection = collection
        
        self.meta = compat_collection(database, collection + '.metadata')
        self.gridfs = gridfs.GridFS(database, collection)
        self.gridfs_files = compat_collection(database, collection + '.files')

        self.active_inodes = collections.defaultdict(int)
        self.active_writes = {}

    def open(self, inode, flags):
        self.logger.debug("open: %s %s", inode, flags)

        # Do not allow writes to a existing file
        if flags & os.O_WRONLY: 
            raise llfuse.FUSEError(errno.EACCES)

        # Deny if write mode and filesystem is mounted as read-only
        #if flags & (os.O_RDWR | os.O_CREAT | os.O_WRONLY | os.O_APPEND) and self._readonly:
        #    raise llfuse.FUSWERROR(errno.EPERM)
        
        self.active_inodes[inode] += 1
        return inode

    def opendir(self, inode):
        """Just to check access, dont care about access => return inode"""
        self.logger.debug("opendir: %s", inode)
        return inode

    def access(self, inode, mode, ctx):
        """Again this fs does not care about access"""
        self.logger.debug("access: %s %s %s", inode, mode, ctx)
        return True

    def getattr(self, inode):
        self.logger.debug("getattr: %s", inode)
        return self._gen_attr(self._entry_by_inode(inode))

    def readdir(self, inode, off):
        self.logger.debug("readdir: %s %s", inode, off)

        entry = self._entry_by_inode(inode)
        for index, child_inode in enumerate(entry.childs.values()[off:]):
            child = self._entry_by_inode(child_inode)
            yield (child.filename, self._gen_attr(child), off + index + 1)

    def lookup(self, folder_inode, name):
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

        return self.getattr(inode)

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
        grid_in = self._create_grid_in(entry)

        self.active_inodes[entry.inode] += 1
        self.active_writes[entry.inode] = grid_in

        return (entry.inode, self._gen_attr(entry))

    def _create_grid_in(self, entry):
        gridfs_filename = self._create_full_path(entry)
        return self.gridfs.new_file(_id=entry.inode, filename=gridfs_filename)

    def _create_full_path(self, entry):
        # Build the full path for this file.
        # Add the full path to make other tools like
        # mongofiles, mod_gridfs, ngx_gridfs happy
        path = collections.deque()
        while entry._id != llfuse.ROOT_INODE:
            path.appendleft(entry.filename)
            entry = self._entry_by_inode(entry.parent_inode)
        path.appendleft(entry.filename)
        return os.path.join(*path)

    def _create_entry(self, folder_inode, name, mode, ctx):
        inode = self._gen_inode()
        entry = Entry(self, name, inode, folder_inode, mode, ctx.uid, ctx.gid)

        self._insert_entry(entry)

        query = {"_id":  folder_inode}
        update = {"$addToSet": {"childs": (name, inode)}}
        self.meta.update_one(query, update)

        return entry

    def setattr(self, inode, attr):
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

    def unlink(self, folder_inode, name):
        self.logger.debug("unlink: %s %s", folder_inode, name)

        self._delete_inode(
            folder_inode,
            name,
            self._delete_inode_check_file)

    def rmdir(self, folder_inode, name):
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

    def read(self, inode, offset, length):
        self.logger.debug("read: %s %s %s", inode, offset, length)

        try:
            grid_out = self.gridfs.get(inode)
        except gridfs.errors.NoFile:
            msg = "Read of inode (%s) fails. Gridfs object not found"
            self.logger.error(msg, inode)
            raise llfuse.FUSEError(errno.EIO)

        grid_out.seek(offset)
        return grid_out.read(length)

    def write(self, inode, offset, data):
        self.logger.debug("write: %s %s %s", inode, offset, len(data))

        # Only 'append once' semantics are supported.

        if inode not in self.active_writes:
            raise llfuse.FUSEError(errno.EINVAL)

        grid_in = self.active_writes[inode]

        if offset != grid_in_size(grid_in):
            raise llfuse.FUSEError(errno.EINVAL)

        grid_in.write(data)
        return len(data)

    def release(self, inode):
        self.logger.debug("release: %s", inode)

        self.active_inodes[inode] -= 1
        if self.active_inodes[inode] == 0:
            del self.active_inodes[inode]
            if inode in self.active_writes:
                self.active_writes[inode].close()
                del self.active_writes[inode]

    def releasedir(self, inode):
        self.logger.debug("releasedir: %s", inode)

    def forget(self, inode_list):
        self.logger.debug("forget: %s", inode_list)

    def readlink(self, inode):
        self.logger.debug("readlink: %s", inode)
        raise llfuse.FUSEError(errno.ENOSYS)

    def symlink(self, folder_inode, name, target, ctx):
        self.logger.debug("symlink: %s %s %s", folder_inode, name, target)
        raise llfuse.FUSEError(errno.ENOSYS)

    def rename(self, old_folder_inode, old_name, new_folder_inode, new_name):
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

    def link(self, inode, new_parent_inode, new_name):
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

    def statfs(self):
        self.logger.debug("statfs")
        raise llfuse.FUSEError(errno.ENOSYS)

    def _entry_by_inode(self, inode):
        query = {'_id': inode}
        record = self.meta.find_one(query)
        return self._doc_to_entry(record or {'childs': []})

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
        doc['childs'] = entry.childs.items()
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

        attr.st_atime = int(entry.atime)
        attr.st_mtime = int(entry.mtime)
        attr.st_ctime = int(entry.ctime)

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
    try:
        ops.meta.create_index(index, unique=True)
    except pymongo.errors.OperationFailure:
        ops.meta.drop()
        _ensure_root_inode(ops)
        _ensure_next_inode_document(ops)
        ops.meta = compat_collection(ops._database, ops._collection + '.metadata')
        ops.meta.create_index(index, unique=False)


def operations_factory(options):
    client = pymongo.MongoClient(options.mongodb_uri)

    ops = Operations(client[options.database], collection=options.collection, logfile=options.logfile)
    _ensure_root_inode(ops)
    _ensure_next_inode_document(ops)
    _ensure_indexes(ops)

    return ops
