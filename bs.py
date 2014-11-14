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
BS_TYPE = 'bs_type'
BS_REF = 'bs_ref'
BS_NAME = 'bs_name'
BS_ATTR_KEYS = [BS_MODE, BS_TYPE, BS_REF, BS_NAME]

BS_TYPE_BLOB = 'blob'
BS_TYPE_TREE = 'tree'
BS_TYPE_PARENT = 'parent'
BS_TYPE_CTIME = 'ctime'


def attr_to_str(attr):
    return '%06o %s %s %s' % tuple(attr[key] for key in BS_ATTR_KEYS)


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
            return attr
    return None


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


def get_root(ref=None):
    if ref is None:
        ref = get_rootref()
    blob = get_blob(ref)
    return dict(line.split() for line in blob.split('\n'))


def get_rootref():
    if not os.path.exists(ROOTREF_PATH):
        return None
    with open(ROOTREF_PATH, 'rb') as f:
        return f.read()


def get_tree(ref, path=None):
    if path is not None:
        ref = blobref_by_path(ref, path)
    for line in get_blob(ref).splitlines():
        attr = dict(zip(BS_ATTR_KEYS, line.split()))
        attr[BS_MODE] = int(attr[BS_MODE], 8)
        yield attr


def index_build(ref, dirpath='/'):
    if dirpath == os.sep:
        print '040755 tree %s %s' % (ref, dirpath)
    for attr in get_tree(ref):
        attr[BS_NAME] = os.path.join(dirpath, attr[BS_NAME])
        print attr_to_str(attr)
        if attr[BS_TYPE] == BS_TYPE_TREE:
            index_build(attr[BS_REF], attr[BS_NAME])


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
            st = os.lstat(path)
            attr = {
                BS_MODE: st.st_mode,
                BS_TYPE: (BS_TYPE_BLOB if os.path.isfile(path) or
                          os.path.islink(path) else BS_TYPE_TREE),
                BS_REF: put_file(path),
                BS_NAME: name,
            }
            ls.append(attr_to_str(attr))
        return '\n'.join(ls)
    if os.path.islink(path):
        blob = os.readlink(path)
    elif os.path.isfile(path):
        f = open(path, 'rb')
        blob = f.read()
        f.close()
    else:
        blob = put_tree(path)
    return put_blob(blob)


def set_attr(rootref, path, new_attr):
    dirpath, filename = os.path.split(path)
    if not filename:
        return new_attr[BS_REF]
    exists = False
    tree = []
    for attr in get_tree(rootref, dirpath):
        if attr[BS_NAME] == filename:
            if BS_REF in new_attr:
                tree.append(new_attr)
            exists = True
        else:
            tree.append(attr)
    if not exists:
        tree.append(new_attr)
    tree.sort(lambda a, b: cmp(a[BS_NAME], b[BS_NAME]))
    ref = put_blob('\n'.join(attr_to_str(attr) for attr in tree))
    if dirpath == os.sep:
        return ref
    attr = get_attr(rootref, dirpath)
    attr[BS_REF] = ref
    return set_attr(rootref, dirpath, attr)


def set_rootref(ref):
    f = open(ROOTREF_PATH, 'wb')
    f.write(ref)
    f.close()


def files_by_path(ref, path):
    return [row[BS_NAME] for row in get_tree(ref, path)]
