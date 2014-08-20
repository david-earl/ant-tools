#!/usr/bin/python

import io, os, sys, json

from subprocess import Popen, PIPE

BIN = "bin/x86_64/ant-tools" 


def main():
  cwd = os.getcwd()
  filename = os.path.join(cwd, BIN) 

  args = sys.argv[1:]

  if not args:
    print 'n'
    args = ['']

  for record in call_bin(filename, *args):
    print json.dumps(record)


def call_bin(command, args):
  command_args = [command, args]

  process = Popen(command_args, stdout=PIPE)

  for line in io.open(process.stdout.fileno()):
    #print line.rstrip('\n')
    yield json.loads(line.rstrip('\n'))


if __name__ == '__main__':
  main()
