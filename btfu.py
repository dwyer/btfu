#!/usr/bin/env python

import argparse
import getpass
import os
import sys

import bs

# import gnupg
# gpg = gnupg.GPG('/usr/local/bin/gpg',
#                 homedir=os.path.join(os.environ.get('HOME'), '.gnupg'))


def btfu_add(args):
    # FIXME: right now `add` just rebuilds the entire root tree. It should be
    # able to add a single file and simply rebuild its parent branches.
    print bs.blob_put('.', is_root=True)


def btfu_cat(args):
    print bs.blob_by_ref(args.ref),


def btfu_init(args):
    bs.init()


def btfu_list(args):
    if args.ref is None:
        args.ref = bs.rootref()
    bs.index_build(args.ref)


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
    subparser = subparsers.add_parser('init')
    subparser.set_defaults(func=btfu_init)
    # list
    subparser = subparsers.add_parser('list')
    subparser.set_defaults(func=btfu_list)
    subparser.add_argument('ref', nargs='?', default=None)
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
