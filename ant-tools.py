#!/usr/bin/python

import io, os, sys, json

from subprocess import Popen, PIPE

BIN = "bin/ant-tools" 


def main():
  cwd = os.getcwd()
  filename = os.path.join(cwd, BIN) 

  args = sys.argv[1:]

  if not args:
    args = ['']
  

  for line in call_bin(filename, *args):
    print json.dumps(record)


def call_bin(command, args):
  command_args = [command, args]

  process = Popen(command_args, stdout=PIPE)

  for line in io.open(process.stdout.fileno()):
    yield ine.rstrip('\n')


if __name__ == '__main__':
  main()
