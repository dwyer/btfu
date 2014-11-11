#!/usr/bin/env python

import argparse
import getpass
import hashlib
import os
import sys
import stat
import time

# import gnupg
# gpg = gnupg.GPG('/usr/local/bin/gpg',
#                 homedir=os.path.join(os.environ.get('HOME'), '.gnupg'))

REPONAME = '.btfu'
REPOPATH = os.path.join('.', REPONAME)
BLOBPATH = os.path.join(REPOPATH, 'blobs')
ROOTREF_PATH = os.path.join(REPOPATH, 'root')

IGNORE_FILES = [REPONAME, '.git']


def attr_by_name(ref, name):
    for obj in tree_by_ref(ref):
        now = time.time()
        if name == obj['nam']:
            attr = dict(
                st_mode=(int(obj['mod'], 8)),
                st_atime=now,
                st_ctime=now,
                st_mtime=now,
                st_nlink=1, # TODO: make this meaningful
            )
            if obj['typ'] == 'tree':
                attr['st_nlink'] += 1
            attr['st_size'] = int(obj['siz'])
            return attr
    return {}


def attr_by_path(ref, path):
    path, name = os.path.split(path)
    ref = blobref_by_path(ref, path)
    return attr_by_name(ref, name)


def blob_by_ref(ref):
    with open(blob_path_by_ref(ref), 'rb') as f:
        return f.read()


def blob_by_path(ref, path):
    return blob_by_ref(blobref_by_path(ref, path))


def blob_put(path):
    if os.path.islink(path):
        blob = os.readlink(path)
    elif os.path.isfile(path):
        f = open(path, 'rb')
        blob = f.read()
        f.close()
    else:
        blob = tree_put(path)
    ref = blobref_by_blob(blob)
    blobpath = os.path.join(BLOBPATH, ref)
    if not os.path.exists(blobpath):
        f = open(blobpath, 'wb')
        f.write(blob)
        f.close()
    return ref


def blobref_by_blob(blob):
    return 'sha1-%s' % hashlib.sha1(blob).hexdigest()


def blobref_by_path(ref, path):
    if not path:
        return ref
    if isinstance(path, basestring):
        path = path[1:]
        path = path.split(os.sep) if path else []
        return blobref_by_path(ref, path)
    name = path.pop(0)
    for obj in tree_by_ref(ref):
        if obj['nam'] == name:
            return blobref_by_path(obj['ref'], path)
    return ref


def blob_path_by_ref(ref):
    return os.path.join(BLOBPATH, ref)


def index_build(ref, dirpath=None):
    if dirpath is None:
        dirpath = '/'
        print '040755 tree %s %s' % (ref, dirpath)
    for attr in tree_by_ref(ref):
        path = os.path.join(dirpath, attr['nam'])
        print '%s %s %s %s' % (attr['mod'], attr['typ'], attr['ref'], path)
        if attr['typ'] == 'tree':
            index_build(attr['ref'], path)


def rootref():
    with open(ROOTREF_PATH, 'rb') as f:
        return f.read()


def tree_by_path(ref, path):
    return tree_by_ref(blobref_by_path(ref, path))


def tree_by_ref(ref):
    keys = ['mod', 'siz', 'typ', 'ref', 'nam']
    for line in blob_by_ref(ref).splitlines():
        yield dict(zip(keys, line.split()))


def tree_make(ref, path, mode):
    newref = blobref_by_blob('')
    path, name = os.path.split(path)
    if name in files_by_path(ref, path):
        return None
    i = 0
    while name:
        print i, path, name
        path, name = os.path.split(path)
        for attr in tree_by_path(ref, path):
            print attr
        i += 1
    return ref


def tree_put(dirpath):
    ls = []
    for name in os.listdir(dirpath):
        if name in IGNORE_FILES:
            continue
        path = os.path.join(dirpath, name)
        ref = blob_put(path)
        if os.path.islink(path):
            typ = 'blob'
        elif os.path.isfile(path):
            typ = 'blob'
        elif os.path.isdir(path):
            typ = 'tree'
        st = os.lstat(path)
        mod = '%06o' % st.st_mode
        siz = str(st.st_size)
        ls.append((mod, siz, typ, ref, name))
    return '\n'.join(' '.join(x) for x in ls)


def files_by_path(ref, path):
    return [row['nam'] for row in tree_by_path(ref, path)]


def btfu_add(args):
    # FIXME: right now `add` just rebuilds the entire root tree. It should be
    # able to add a single file and simply rebuild its parent branches.
    ref = blob_put('.')
    f = open(ROOTREF_PATH, 'wb')
    f.write(ref)
    f.close()
    print ref


def btfu_cat(args):
    print blob_by_ref(args.ref),


def btfu_index(args):
    index_build(args.ref)


def btfu_init(args):
    try:
        os.mkdir(REPOPATH)
        os.mkdir(BLOBPATH)
    except OSError, e:
        print e


def btfu_list(args):
    print btfu.repo.files()


def btfu_rootref(args):
    print # rootref


def main(args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    # add
    subparser = subparsers.add_parser('add')
    subparser.set_defaults(func=btfu_add)
    # cat
    subparser = subparsers.add_parser('cat')
    subparser.set_defaults(func=btfu_cat)
    subparser.add_argument('ref')
    # init
    subparser = subparsers.add_parser('index')
    subparser.set_defaults(func=btfu_index)
    subparser.add_argument('ref')
    # init
    subparser = subparsers.add_parser('init')
    subparser.set_defaults(func=btfu_init)
    # list
    subparser = subparsers.add_parser('list')
    subparser.set_defaults(func=btfu_list)
    # root
    subparser = subparsers.add_parser('rootref')
    subparser.set_defaults(func=btfu_rootref)
    # ...
    args = parser.parse_args(args)
    func = args.func
    del args.func
    func(args)


if __name__ == '__main__':
    main(sys.argv[1:])
