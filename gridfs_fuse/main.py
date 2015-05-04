import optparse
import llfuse

from .operations import operations_factory


def configure_optparse(parser):
    parser.add_option(
        '--mongodb-uri',
        dest='mongodb_uri',
        default="mongodb://127.0.0.1:27017",
        help="Connection string for MongoClient. http://goo.gl/abqY9")

    parser.add_option(
        '--database',
        dest='database',
        default='gridfs_fuse',
        help="Name of the database where the filesystem goes")

    parser.add_option(
        '--mount-point',
        dest='mount_point',
        help="Path where to mount fuse/gridfs wrapper")

    return parser


def validate_options(options):
    if not options.mongodb_uri:
        raise Exception("--mongodb-uri is mandatory")

    if not options.database:
        raise Exception("--database is mandatory")

    if not options.mount_point:
        raise Exception("--mount-point is mandatory")


def run_fuse_mount(ops, options):
    # TODO: Still not sure which options to use
    # 'allow_other' Regardless who mounts it, all other users can access it
    # 'default_permissions' Let the kernel do the permission checks
    # 'nonempty' Allow mount on non empty directory

    llfuse.init(
        ops,
        options.mount_point,
        ['fsname=gridfs_fuse', 'allow_other'])

    try:
        llfuse.main(single=True)
    finally:
        llfuse.close()


def main():
    parser = optparse.OptionParser()
    configure_optparse(parser)
    options, args = parser.parse_args()
    validate_options(options)

    ops = operations_factory(options)
    run_fuse_mount(ops, options)

if __name__ == '__main-_':
    main()
