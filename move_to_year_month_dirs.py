#!/usr/bin/env python3
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


def move_group(groupName):
  h = logging.StreamHandler()
  h.setFormatter(logging.Formatter(
     '[%(levelname)s %(asctime)s %(name)s] %(message)s'))
  logger = logging.getLogger(groupName)
  logger.propagate = False
  logger.addHandler(h)
  logger.setLevel(logging.INFO)
  for fname in glob.glob(os.path.join(groupName, "*.json")):
    with open(fname, "rb") as fh:
      try:
        # Rename to groupName/year/month/msgid.json if postDate is available.
        postDate = datetime.datetime.fromtimestamp(
          int(json.loads(fh.read())['ygData']['postDate']))
        destDir = os.path.join(
          groupName, postDate.strftime('%Y'), postDate.strftime('%m'))
        os.makedirs(destDir, exist_ok=True)
        dest = os.path.join(destDir, os.path.basename(fname))
        os.rename(fname, dest)
      except json.decoder.JSONDecodeError as e:
        logger.error('%s %s', fname, e)
      except AttributeError as e:  # .get
        logger.error('%s %s', fname, e)
      except TypeError as e:  # ['key']
        logger.error('%s %s', fname, e)
      except ValueError as e:  # int parsing
        logger.error('%s %s', fname, e)


if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.stderr.write('Usage: %s <group_name>\n' % __file__)
    sys.exit(1)
  groupName = sys.argv[1]
  move_group(groupName)
