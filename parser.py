import argparse

_parser = argparse.ArgumentParser()
_parser.add_argument('-b', '--branch', default="master")
_parser.add_argument('-c', '--commit', default="HEAD")
_parser.add_argument('-r', '--repository', default="https://github.com/apache/iotdb")
args = _parser.parse_args()
