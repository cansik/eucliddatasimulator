import os
import argparse
from euclid_stubs_generator.stubs_generator import StubsGenerator

__author__ = 'cansik'


def parse_cmd_args():
    parser = argparse.ArgumentParser(description="Utility generating test stubs for executables.")
    parser.add_argument("--pkgdefs", help="Path to folder that contains the package definitions (package repository).")
    parser.add_argument("--destdir", help="Directory to write the test stubs to.")
    parser.add_argument("--xml", dest='xml', action='store_true', help="Specify flag to generate xml output (otherwise text is produced; note that lists are always pickled).")
    args = parser.parse_args()
    args.pkgdefs = os.path.expandvars(args.pkgdefs)
    args.destdir = os.path.expandvars(args.destdir)

    return args


def main():
    args = parse_cmd_args()

    generator = StubsGenerator(args.destdir)
    generator.generate_stubs_from_folder(args.pkgdefs)


if __name__ == '__main__':
    main()
