import fnmatch
import hashlib
import os
import stat
import time


REPONAME = '.btfu'
REPOPATH = os.path.join('.', REPONAME)
BLOBPATH = os.path.join(REPOPATH, 'blobs')
ROOTREF_PATH = os.path.join(REPOPATH, 'root')

IGNORE_FILES = [
    REPONAME,
    '.git',
    '*.pyc',
]


def attr_by_name(ref, name):
    # TODO: move this over to fs.py
    for obj in get_tree(ref):
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
    # TODO: move this over to fs.py
    path, name = os.path.split(path)
    ref = blobref_by_path(ref, path)
    return attr_by_name(ref, name)


def blob_by_path(ref, path):
    return get_blob(blobref_by_path(ref, path))


def blobref(blob):
    return 'sha1-%s' % hashlib.sha1(blob).hexdigest()


def blobref_by_path(ref, path):
    if not path:
        return ref
    if isinstance(path, basestring):
        path = path[1:]
        path = path.split(os.sep) if path else []
        return blobref_by_path(ref, path)
    name = path.pop(0)
    for obj in get_tree(ref):
        if obj['nam'] == name:
            return blobref_by_path(obj['ref'], path)
    return ref


def blob_path_by_ref(ref):
    return os.path.join(BLOBPATH, ref)


def get_blob(ref):
    with open(blob_path_by_ref(ref), 'rb') as f:
        return f.read()


def get_tree(ref):
    keys = ['mod', 'siz', 'typ', 'ref', 'nam']
    for line in get_blob(ref).splitlines():
        yield dict(zip(keys, line.split()))


def index_build(ref, dirpath='/'):
    if dirpath == os.sep:
        print '040755 tree %s %s' % (ref, dirpath)
    for attr in get_tree(ref):
        path = os.path.join(dirpath, attr['nam'])
        print '%s %s %s %s' % (attr['mod'], attr['typ'], attr['ref'], path)
        if attr['typ'] == 'tree':
            index_build(attr['ref'], path)


def init():
    try:
        os.mkdir(REPOPATH)
        os.mkdir(BLOBPATH)
    except OSError, e:
        print e


def put_blob(blob):
    ref = blobref(blob)
    blobpath = os.path.join(BLOBPATH, ref)
    if not os.path.exists(blobpath):
        f = open(blobpath, 'wb')
        f.write(blob)
        f.close()
    return ref


def put_file(path, is_root=True):
    def ignore(name):
        for glob in IGNORE_FILES:
            if fnmatch.fnmatch(name, glob):
                return True
        return False
    def put_tree(dirpath):
        ls = []
        for name in os.listdir(dirpath):
            if ignore(name):
                continue
            path = os.path.join(dirpath, name)
            ref = put_file(path)
            if os.path.isfile(path) or os.path.islink(path):
                typ = 'blob'
            elif os.path.isdir(path):
                typ = 'tree'
            st = os.lstat(path)
            mod = '%06o' % st.st_mode
            siz = str(st.st_size)
            ls.append((mod, siz, typ, ref, name))
        return '\n'.join(' '.join(x) for x in ls)
    if os.path.islink(path):
        blob = os.readlink(path)
    elif os.path.isfile(path):
        f = open(path, 'rb')
        blob = f.read()
        f.close()
    else:
        blob = put_tree(path)
    ref = put_blob(blob)
    if is_root:
        f = open(ROOTREF_PATH, 'wb')
        f.write(ref)
        f.close()
    return ref


def rootref():
    with open(ROOTREF_PATH, 'rb') as f:
        return f.read()


def tree_by_path(ref, path):
    return get_tree(blobref_by_path(ref, path))


def tree_make(ref, path, mode):
    newref = blobref('')
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


def files_by_path(ref, path):
    return [row['nam'] for row in tree_by_path(ref, path)]
