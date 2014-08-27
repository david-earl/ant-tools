#!/usr/bin/python

import io, os, sys, json
from subprocess import Popen, PIPE

BIN = "ant-tools" 


class AntReader(object):
  def __init__(self, ant_path):
    if not ant_path:
      raise Exception("An .ant file must be specified")

    self.ant_path = ant_path

  def Load(self):
    for line in call_bin(get_bin_path(), self.ant_path):
      yield json.loads(line)

  def Validate(self):
    for line in call_bin(get_bin_path(), [self.ant_path, "--validate"]):
      print line

  def PrintStats(self):
    for line in call_bin(get_bin_path(), [self.ant_path, "--stats"]):
      print line


def main():

  args = sys.argv[1:]

  if not args:
    args = ['']

  for line in call_bin(get_bin_path(), args):
    if not line:
      continue
    print line

def get_bin_path():
  return os.path.join(os.getcwd(), BIN) 

def call_bin(command, args):
  command_args = [command] + args

  process = Popen(command_args, stdout=PIPE)

  for line in io.open(process.stdout.fileno()):
    yield line.rstrip('\n')


if __name__ == '__main__':
  main()
