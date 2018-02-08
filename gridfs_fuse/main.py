'''
Mounts a GridFS filesystem using FUSE in Python
'''
import logging
import argparse
import llfuse
import os
import sys

from pymongo.uri_parser import parse_uri

from .operations import operations_factory

FUSE_OPTIONS_HELP='''
FUSE options for mount (comma-separated) [default: %(default)s]. 
  debug - turn on detailed debugging. 
  workers=N - number of workers [default: 1]. 
  single - equivalent to workers=1 for llfuse compatibility. 
  log_level=LEVEL - specifies the logging level. 
  log_file=FILE - specifies path for loging to file. 
  foreground - run process in foreground rather than as daemon process. 

Note: Generic options can be found at: http://man7.org/linux/man-pages/man8/mount.fuse.8.html
'''

class HelpFormatter(argparse.HelpFormatter):
    '''A custom formatter to rearrange order of positionals 
       and hide actions starting with _'''
    # use defined argument order to display usage
    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = 'usage: '

        # if usage is specified, use that
        if usage is not None:
            usage = usage % dict(prog=self._prog)

        # if no optionals or positionals are available, usage is just prog
        elif usage is None and not actions:
            usage = '%(prog)s' % dict(prog=self._prog)
        elif usage is None:
            prog = '%(prog)s' % dict(prog=self._prog)
            # build full usage string
            actions_list = []
            for a in actions:
                if len(a.option_strings) > 0:
                    actions_list.append(a)
                elif a.dest == 'help':
                    actions_list.insert(0, a)
                elif a.dest.startswith('_'):
                    print('skipped {}'.format(a))
                    pass  # hide these 
                else:
                    actions_list.insert(1, a) if len(actions_list) else actions_list.append(a)
            action_usage = self._format_actions_usage(actions_list, groups) # NEW
            usage = ' '.join([s for s in [prog, action_usage] if s])
            # omit the long line wrapping code
        # prefix with 'usage:'
        return '%s%s\n\n' % (prefix, usage)

    def _format_action(self, action):
        if not action.dest.startswith('_'):
            return super(self.__class__, self)._format_action(action) 

class OrderedNamespace(argparse.Namespace):
    '''Allows argument order to be retained'''
    def __init__(self, **kwargs):
        self.__dict__["_arg_order"] = []
        self.__dict__["_arg_order_first_time_through"] = True
        argparse.Namespace.__init__(self, **kwargs)

    def __setattr__(self, name, value):
        #print("Setting %s -> %s" % (name, value))
        self.__dict__[name] = value
        if name in self._arg_order and hasattr(self, "_arg_order_first_time_through"):
            self.__dict__["_arg_order"] = []
            delattr(self, "_arg_order_first_time_through")
        self.__dict__["_arg_order"].append(name)

    def _finalize(self):
        if hasattr(self, "_arg_order_first_time_through"):
            self.__dict__["_arg_order"] = []
            delattr(self, "_arg_order_first_time_through")

    def _latest_of(self, k1, k2):
        try:
            print self._arg_order
            if self._arg_order.index(k1) > self._arg_order.index(k2):
                return k1
        except ValueError:
            if k1 in self._arg_order:
                return k1
        return k2


def configure_parser(parser):
    '''Configures CLI options'''
    parser.add_argument(
        '-m', '--mount-point',
        dest='mount_point', 
        help="Path where to mount fuse/gridfs wrapper")

    parser.add_argument(
        '-u', '--mongodb-uri',
        dest='mongodb_uri', 
        default="mongodb://127.0.0.1:27017/gridfs_fuse.fs",
        help="""Connection string for MongoClient. http://goo.gl/abqY9 "
             "[default: %(default)s]""")

    parser.add_argument(
        '-d', '--database',
        dest='database',
        default='gridfs_fuse', 
        help="Name of the database where the filesystem goes [default: %(default)s]")

    parser.add_argument(
        '-c', '--collection', dest='collection', default='fs', 
        help='Database collection for GridFS [default: %(default)s]')

    parser.add_argument(
        '-o', '--options', dest='mount_opts', action='append', 
        default=['default_permissions'], 
        help=FUSE_OPTIONS_HELP)

    parser.add_argument(
        '-l', '--log', dest='logfile', default=os.devnull, 
        const='gridfs_fuse.log', nargs='?',
        help='Log actions to file [default: %(default)s]')

    return parser

def fuse_configurator(parser):
    '''Configure parser for mount CLI style of form: <srv> <mnt_pt> [-o <options>]'''
    parser.add_argument('_script_path')  # hack to fix ordering

    parser.add_argument('mongodb_uri',
        help="MongoDB connection URI in form "
             "'mongodb://[user:password@]hostname[:port]/db.collection'")

    parser.add_argument('mount_point',
        help="Path to mount fuse gridfs filesystem")

    parser.add_argument(
        '-o', dest='mount_opts', action='append', 
        default=['default_permissions'], help=FUSE_OPTIONS_HELP)

    return parser

def validate_options(options):
    '''Validates parser arguments'''
    uri = parse_uri(options.mongodb_uri)
    options.database = uri.get('database', options.database)
    options.collection = uri.get('collection', options.collection)
    if not options.mount_point:
        raise Exception("mount_point is mandatory")

def fuse_validator(options):
    '''Validates parser arguments using mount interface'''
    options.database = 'gridfs_fuse'
    options.collection = 'fs'
    validate_options(options)
    opts = dict([opt.split('=', 1) if '=' in opt else (opt, None) 
                 for opt in options.mount_opts])
    options.logfile = opts.get('log_file', None)

# shamelessly *adapted* from the the borg collective (see - borgbackup project)
def daemonize():
    """Detach process from controlling terminal and run in background
    Returns: old and new get_process_id tuples
    """
    old_id = os.getpid() 
    pid = os.fork()
    if pid:
        os._exit(0)
    os.setsid()
    pid = os.fork()
    if pid:
        os._exit(0)
    new_id = os.getpid()
    return old_id, new_id


def run_fuse_mount(ops, options, mount_opts):
    '''Performs FUSE mount'''
    mount_opts = ['fsname=gridfs'] + mount_opts
    opts = dict((opt.split('=', 1) if '=' in opt else (opt, None) for opt in mount_opts))

    # strip invalid keys
    ignored_keys = ['debug', 'foreground', 'log_level', 'log_file', 'workers', 'single']
    valid_keys = [k for k in opts if k not in ignored_keys]
    mount_opts = ['='.join([k, opts[k]]) if opts[k] is not None else k for k in valid_keys]

    # handle some key options here
    if 'log_level' in opts:
        try:
            log_level = opts['log_level'].upper()
            try:
                log_level = int(log_level)
            except ValueError:
                pass
            logging.getLogger().setLevel(getattr(logging, log_level))
        except (TypeError, ValueError) as error:
            logging.warning('Unable to set log_level to {}: {}'.format(opts['log_level'], error)) 

    # start gridfs bindings and run fuse process
    llfuse.init(ops, options.mount_point, mount_opts)
    
    # ensure that is single is given then it evaluates to true
    if 'single' in opts and opts['single'] is None:
        opts['single'] = True

    # debug clobbers other log settings such as log_level
    if 'debug' in opts:
        logging.basicConfig(
            format='[%(asctime)s] pid=%(process)s {%(module)s:%(funcName)s():%(lineno)d} %(levelname)s - %(message)s',
            level=logging.DEBUG)

    
    # TODO: Find way of capturing CTRL+C and calling llfuse.close() when in foreground
    # Note: This maybe a bug in llfuse
    workers = opts.get('workers', opts.get('single', 1))  # fudge for backwards compatibility  
    try:
        llfuse.main(workers)  # maintain compatibility with single/workers kwarg
    except KeyboardInterrupt:
        pass
    finally:
        llfuse.close()


def init(args, configure=configure_parser, validate=validate_options):
    '''Initialise using specified parser config and validation'''
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        #format='[%(asctime)s] pid=%(process)s {%(module)s:%(funcName)s():%(lineno)d} %(levelname)s - %(message)s',
        level=logging.INFO)

    parser = argparse.ArgumentParser(formatter_class=HelpFormatter)
    configure(parser)
    options, _ = parser.parse_known_args(args, namespace=OrderedNamespace())
    
    # flatten options list
    flatten = lambda l: [item for sublist in l for item in sublist.split(',')]
    options.mount_opts = flatten(options.mount_opts)
    
    validate(options)

    # have to fork process before creating MongoClient object otherwise safety warnings
    if 'foreground' not in options.mount_opts:
        pids = daemonize()  # make the program run as non-blocking process
        logging.debug('Daemonized parent process {} with child process {}'.format(*pids))

    ops = operations_factory(options)

    # TODO: Still not sure which options to use
    # 'allow_other' Regardless who mounts it, all other users can access it
    # 'default_permissions' Let the kernel do the permission checks
    # 'nonempty' Allow mount on non empty directory
    mount_opts = options.mount_opts

    run_fuse_mount(ops, options, mount_opts)


def main(args=sys.argv):
    '''Default interface'''
    init(args, configure=configure_parser, validate=validate_options)  # defaults

def _mount_fuse_main(args=sys.argv):
    '''Interface for mount.fuse'''
    init(args, configure=fuse_configurator, validate=fuse_validator) 

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass

