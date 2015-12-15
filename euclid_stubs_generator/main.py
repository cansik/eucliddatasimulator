import os
import argparse
from euclidwf.utilities import exec_loader

from euclid_stubs_generator.mock_generator import MockGenerator
from euclid_stubs_generator.stubs_generator import StubsGenerator

__author__ = 'cansik'


def parse_cmd_args():
    parser = argparse.ArgumentParser(
            description="Utility generating test stubs for executables.")
    parser.add_argument("--pkgdefs",
                        help="Path to folder that contains the package definitions (package repository).")
    parser.add_argument("--destdir",
                        help="Directory to write the test stubs to.")
    parser.add_argument("--xml", dest='xml', action='store_true',
                        help="Specify flag to generate xml output (otherwise text is produced; "
                             "note that lists are always pickled).")
    args = parser.parse_args()
    args.pkgdefs = os.path.expandvars(args.pkgdefs)
    args.destdir = os.path.expandvars(args.destdir)

    return args


def main():
    args = parse_cmd_args()

    generator = StubsGenerator(args.destdir)

    executables = exec_loader.get_all_executables(args.pkgdefs)

    test_pipeline_name = 'vis_split_quadrants'

    executables = dict({(k, v) for k, v in executables.items() if
                        k == test_pipeline_name})

    generator.generate_stubs(executables,
                             {test_pipeline_name: {'quadrants_list': 20}})

    mock_output = os.path.join(args.destdir, 'mock')
    mocker = MockGenerator(mock_output)
    mocker.generate_mocks({'vis_split': 50, 'vis_combine': 100})


if __name__ == '__main__':
    main()
