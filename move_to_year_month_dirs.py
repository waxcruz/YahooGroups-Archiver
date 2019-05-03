#!/usr/bin/env python
"""
Move files from groupName/*.json to groupName/year/month/*.json based on the postDate
in the JSON file.
"""

import datetime
import json
import logging
import os
import re
import sys
import traceback


# groups: 1: message ID
JSON_ID = re.compile(r'(\d+)\.json$', re.IGNORECASE)


class Msg(object):
  def __init__(self, msgId, path, postDate):
    self.id = msgId
    self.path = path
    self.postDate = postDate


def move_group(groupName):
  h = logging.StreamHandler()
  h.setFormatter(logging.Formatter(
     '[%(levelname)s %(asctime)s %(name)s] %(message)s'))
  logger = logging.getLogger(groupName)
  logger.propagate = False
  logger.addHandler(h)
  logger.setLevel(logging.INFO)
  logger.info('Scanning %s...', groupName)
  for dirpath, _, filenames in os.walk(groupName, followlinks=True):
    for fname in filenames:
      m = JSON_ID.match(fname)
      if not m:
        continue
      path = os.path.join(dirpath, fname)
      msg = Msg(int(m.group(1)), path, None)
      with open(path, 'r') as fh:
        try:
          msg.postDate = datetime.datetime.fromtimestamp(
              int(json.load(fh)['ygData']['postDate']))
        except:
          logger.error('Failed to extract postDate from %s: %s', path,
                       traceback.format_exc())
      if msg.postDate:
        destDir = os.path.join(
          groupName, msg.postDate.strftime('%Y'), msg.postDate.strftime('%m'))
      else:
        destDir = groupName
      if not os.path.exists(destDir):
        os.makedirs(destDir)
      destPath = os.path.join(destDir, '%d.json' % msg.id)
      if destPath == msg.path:
        logger.info('Not moving %s', msg.path)
      elif os.path.exists(destPath):
        logger.warn('Refusing to clobber file at %s with file at %s',
                    destPath, msg.path)
      else:
        logger.info('Moving %s to %s', msg.path, destPath)
        os.rename(msg.path, destPath)
        msg.path = destPath


if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.stderr.write('Usage: %s <group_name>\n' % __file__)
    sys.exit(1)
  groupName = sys.argv[1]
  move_group(groupName)
