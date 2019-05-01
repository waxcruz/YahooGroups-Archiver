#!/usr/bin/env python
"""
Move files from groupName/*.json to groupName/year/month/*.json based on the postDate
in the JSON file.
"""

import datetime
import glob
import json
import logging
import os
import sys
import traceback


def move_group(groupName):
  h = logging.StreamHandler()
  h.setFormatter(logging.Formatter(
     '[%(levelname)s %(asctime)s %(name)s] %(message)s'))
  logger = logging.getLogger(groupName)
  logger.propagate = False
  logger.addHandler(h)
  logger.setLevel(logging.INFO)
  for fname in glob.glob(os.path.join(groupName, "*.json")):
    with open(fname, "r") as fh:
      try:
        # Rename to groupName/year/month/msgid.json if postDate is available.
        postDate = datetime.datetime.fromtimestamp(
          int(json.load(fh)['ygData']['postDate']))
        destDir = os.path.join(
          groupName, postDate.strftime('%Y'), postDate.strftime('%m'))
        if not os.path.exists(destDir):
          os.makedirs(destDir)
        dest = os.path.join(destDir, os.path.basename(fname))
        os.rename(fname, dest)
      except:
        logger.error('%s %s', fname, traceback.format_exc())


if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.stderr.write('Usage: %s <group_name>\n' % __file__)
    sys.exit(1)
  groupName = sys.argv[1]
  move_group(groupName)
