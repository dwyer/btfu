import fnmatch
import hashlib
import os
import stat


REPONAME = '.btfu'
REPOPATH = os.path.join('.', REPONAME)
BLOBPATH = os.path.join(REPOPATH, 'blobs')
ROOTREF_PATH = os.path.join(REPOPATH, 'root')

IGNORE_FILES = [
    REPONAME,
    '.git',
    '*.pyc',
]

BS_MODE = 'st_mode'
BS_SIZE = 'st_size'
BS_TYPE = 'bs_type'
BS_REF = 'bs_ref'
BS_NAME = 'bs_name'
BS_ATTR_KEYS = [BS_MODE, BS_SIZE, BS_TYPE, BS_REF, BS_NAME]


def attr_to_str(attr):
    print '%06o %d %s %s %s' % (attr[key] for key in BS_ATTR_KEYS)


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
        if obj[BS_NAME] == name:
            return blobref_by_path(obj[BS_REF], path)
    return ref


def get_attr(ref, path):
    path, name = os.path.split(path)
    ref = blobref_by_path(ref, path)
    for attr in get_tree(ref):
        if name == attr[BS_NAME]:
            st_nlink = 1
            if attr[BS_TYPE] == 'tree':
                st_nlink += 1 # TODO: make this meaningful
            return dict(attr, st_nlink=st_nlink)
    return {}


def get_blob(ref, path=None, size=-1, offset=0):
    if path is not None:
        ref = blobref_by_path(ref, path)
    with open(get_blobpath(ref), 'rb') as f:
        f.seek(offset)
        return f.read(size)


def get_blobpath(ref):
    return os.path.join(BLOBPATH, ref)


def get_blobsize(ref):
    return os.stat(get_blobpath(ref)).st_size


def get_rootref():
    with open(ROOTREF_PATH, 'rb') as f:
        return f.read()


def get_tree(ref, path=None):
    if path is not None:
        ref = blobref_by_path(ref, path)
    for line in get_blob(ref).splitlines():
        attr = dict(zip(BS_ATTR_KEYS, line.split()))
        attr[BS_MODE] = int(attr[BS_MODE], 8)
        attr[BS_SIZE] = int(attr[BS_SIZE])
        yield attr


def index_build(ref, dirpath='/'):
    if dirpath == os.sep:
        print '040755 tree %s %s' % (ref, dirpath)
    for attr in get_tree(ref):
        path = os.path.join(dirpath, attr[BS_NAME])
        print '%06o %s %s %s' % (attr[BS_MODE], attr[BS_TYPE], attr[BS_REF],
                                 path)
        if attr[BS_TYPE] == 'tree':
            index_build(attr[BS_REF], path)


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
        os.chmod(blobpath, 0400)
    return ref


def put_file(path):
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
    return put_blob(blob)


def set_attr(rootref, path, attr):
    if path == os.sep:
        return ref
    dirpath, filename = os.path.split(path)
    exists = False
    tree = []
    for row in get_tree(rootref, dirpath):
        if row[BS_NAME] == filename:
            tree.append(dict(
                mod=oct(attr[BS_MODE]),
                siz=str(attr[BS_SIZE]),
                typ=row[BS_TYPE],
                ref=attr[BS_REF],
                nam=row[BS_NAME],
            ))
            exists = True
        else:
            tree.append(row)
    if not exists:
        # TODO: handle non-existance
        return rootref
    blob = '\n'.join(' '.join((attr[BS_MODE], attr[BS_SIZE], attr[BS_TYPE],
                               attr[BS_REF], attr[BS_NAME])) for attr in tree)
    ref = put_blob(blob)
    return set_fileref(rootref, dirpath, ref)


def set_fileref(rootref, path, ref):
    if path == os.sep:
        return ref
    dirpath, filename = os.path.split(path)
    tree = list(get_tree(rootref, dirpath))
    exists = False
    for attr in tree:
        if attr[BS_NAME] == filename:
            attr[BS_REF] = ref
            attr[BS_SIZE] = str(get_blobsize(ref))
            exists = True
            break
    if not exists:
        # TODO: handle non-existance
        return rootref
    blob = '\n'.join(' '.join((attr[BS_MODE], attr[BS_SIZE], attr[BS_TYPE],
                               attr[BS_REF], attr[BS_NAME])) for attr in tree)
    ref = put_blob(blob)
    return set_fileref(rootref, dirpath, ref)


def set_rootref(ref):
    f = open(ROOTREF_PATH, 'wb')
    f.write(ref)
    f.close()


def tree_make(ref, path, mode):
    newref = blobref('')
    path, name = os.path.split(path)
    if name in files_by_path(ref, path):
        return None
    i = 0
    while name:
        print i, path, name
        path, name = os.path.split(path)
        for attr in get_tree(ref, path):
            print attr
        i += 1
    return ref


def files_by_path(ref, path):
    return [row[BS_NAME] for row in get_tree(ref, path)]
