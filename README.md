# python gridfs fuse
A FUSE wrapper around MongoDB gridfs using python and llfuse.

## Usage
```bash
gridfs_fuse --mongodb-uri="mongodb://127.0.0.1:27017" --database="gridfs_fuse" --mount-point="/mnt/gridfs_fuse"
```

## Requirements
 * pymongo
 * llfuse

## Operations supported
 * create/list/delete directories => folder support.
 * read files.
 * delete files.
 * open and write once (like HDFS).
 * rename


## Operations not supported
 * modify an existing file.
 * resize an existing file.
 * hardlink
 * symlink
 * statfs


## Performance
### Setup
* AWS d2.xlarge machine.
  * 4 @ 2.40Ghz (E5-2676)
  * 30 gigabyte RAM
* filesystem: ext4
* block device: three instance storage disks combined with lvm.
```
lvcreate -L 3T -n mongo -i 3 -I 4096 ax /dev/xvdb /dev/xvdc /dev/xvdd
```
* mongodb 3.0.1
* mongodb storage engine WiredTiger
* mongodb compression: snappy
* mongodb cache size: 10 gigabyte

### Results
* sequential write performance: ~46 MB/s
* sequential read performance: ~90 MB/s

Write performance was tested by copying 124 files, each having a size of 9 gigabytes and different content.
Compression factor was about factor three.
Files were copied one by one => no parallel execution.

Read performance was tested by randomly picking 10 files out of the 124.
Files were read one by one => no parallel execution.

```bash
# Simple illustration of the commands used (not the full script).

# Write
pv -pr /tmp/big_file${file_number} /mnt/gridfs_fuse/

# Read
pv -pr /mnt/gridfs_fuse${file_number} > /dev/null
```
