#!/usr/bin/env python

'''
Yahoo-Groups-Archiver
Original Copyright 2015, 2017, 2018 Andrew Ferguson and others
Mostly rewritten in 2019 by Dan Born

YahooGroups-Archiver, a simple python script that allows for all
messages in a public Yahoo Group to be archived.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''


import datetime
import json
import logging
import requests
import os
import re
from   six.moves import xrange
import sys
import shutil
import sys
import time
import traceback


# Avoid being spammed by Yahoo by looking like a browser.
headers = {'User-agent': 'Mozilla/5.0'}


# If the Yahoo Group is private, login to Yahoo in your browser, find the 'T'
# and 'Y' cookies, and set them in your environment variables. E.g.,
#
# YAHOO_COOKIE_T='...' YAHOO_COOKIE_Y='...' ./archive_group.py ...
cookie_T = os.environ.get('YAHOO_COOKIE_T')
cookie_Y = os.environ.get('YAHOO_COOKIE_Y')

# Set cookie_T and cookie_Y above if required. Don't change this.
cookies = {'T': cookie_T, 'Y': cookie_Y} if cookie_T and cookie_Y else None


# Wait at least minWaitTime between requests to avoid being spammed by Yahoo.
# On errors, double the wait time after every consecutive error, until a
# request is successful, or maxWaitTime is reached. Reset wait time to
# minWaitTime after a successful request.
minWaitTime = datetime.timedelta(milliseconds=100)
maxWaitTime = datetime.timedelta(seconds=10)

# Quit after maxServerErrors consecutive server (500) errors.
maxServerErrors = 10


class Error(Exception):
  pass


class GroupArchiver(object):
  def __init__(self, groupName, log_handler, http):
    """Archive Yahoo group
    :param groupName: name of Yahoo group
    :param log_handler: log handler
    :param http: requests.Session() object"""
    self.groupName = groupName
    self.logger = logging.getLogger(groupName)
    self.logger.propagate = False
    self.logger.addHandler(log_handler)
    self.logger.setLevel(logging.INFO)
    self.http = http
    # msgNumber of messages already archived.
    self.archived = set()

  def archive_group(self, mode):
    self.logger.info("Archiving group '%s' in mode '%s'", self.groupName, mode)
    startTime = datetime.datetime.now()
    # Implicit first number is 1.
    lastMsgNumber = self.group_messages_last()

    # Set msgIds to 3-tuple of (start, stop, step), for xrange.
    if mode == "restart":  # TODO: implement "reverserestart"?
      #delete all previous archival attempts and archive everything again
      self.logger.info("Clearing directory %s...", self.groupName)
      if os.path.exists(self.groupName):
        shutil.rmtree(self.groupName)
      msgIds = (1, lastMsgNumber + 1, 1)
    elif mode in ["update", "retry", "reverseupdate", "reverseretry"]:
      self.logger.info("Scanning %s/... for existing files...", self.groupName)
      minMsgNumber = lastMsgNumber + 1
      maxMsgNumber = 0
      if os.path.exists(self.groupName):
        for _, _, filenames in os.walk(self.groupName, followlinks=True):
          for fname in filenames:
            if fname.endswith(".json"):
              msgNumber = int(fname[0:-5])
              self.archived.add(msgNumber)
              if maxMsgNumber is None or msgNumber > maxMsgNumber:
                maxMsgNumber = msgNumber
              if minMsgNumber is None or msgNumber < minMsgNumber:
                minMsgNumber = msgNumber
      self.logger.info(("Found %d messages. Lowest and highest message " +
                        "numbers (contiguity not checked): %s...%s"),
                         len(self.archived), minMsgNumber, maxMsgNumber)
      
      if mode == "update":
        #start archiving at the last+1 message message we archived
        msgIds = (maxMsgNumber + 1, lastMsgNumber + 1, 1)
      elif mode == "retry":
        #don't archive any messages we already have
        #but try to archive ones that we don't, and may have
        #already attempted to archive
        msgIds = (1, lastMsgNumber + 1, 1)
      elif mode == "reverseupdate":
        msgIds = (minMsgNumber - 1, 0, -1)
      elif mode == "reverseretry":
        msgIds = (lastMsgNumber, 0, -1)
    else:
      sys.stderr.write(
"""You have specified an invalid mode (""" + mode + """)
Valid modes are:
update - add any new messages to the archive
retry - attempt to get all messages that are not in the archive
restart - delete archive and start from scratch\n""")
      sys.exit(1)

    if not os.path.exists(self.groupName):
      os.makedirs(self.groupName)
    self.logger.info("Archiving messages %d...%d",
                       msgIds[0], msgIds[1] - msgIds[2])
    msgsArchived = self.archive_messages(msgIds)
    endTime = datetime.datetime.now()
    self.logger.info("Archive finished, archived %d, time taken is %s", msgsArchived,
                       endTime - startTime)


  def group_messages_last(self):
    """Return ID of last message in self.groupName. The first message ID is 1."""
    resp = self.http.get(
      ('https://groups.yahoo.com/api/v1/groups/%s/messages' % self.groupName) +
         '?count=1&sortOrder=desc&direction=-1', headers=headers, cookies=cookies)
    if resp.status_code != 200:
      raise Error('Failed to fetch last message ID with response code %d' %
                    resp.status_code)
    try:
      pageJson = json.loads(resp.text)
    except:
      truncated = resp.text[:200]
      if re.search(r'yahoo.*?login', truncated, re.IGNORECASE):
        #the user needs to be signed in to Yahoo
        sys.stderr.write(
  """Error. The group you are trying to archive is a private group.
To archive a private group using this tool, login to a Yahoo account that
has access to the private groups, then extract the data from the cookies Y
and T from the domain yahoo.com . Paste this data into the appropriate
variables (cookie_Y and cookie_T) at the top of this script, and run the
script again.\n""")
        sys.exit(2)
      else:
        sys.stderr.write(("Could not get last message ID in group because response " +
                          "could not be parsed as JSON: %s\n%s\n") %
                          (traceback.format_exc(), truncated.encode('utf-8')))
        raise sys.exc_info()[1]
    return pageJson["ygData"]["lastRecordId"]

  def archive_messages(self, msgIds):
    """Archive all msgNumbers in self.groupName. Return number of messages
    successfully archived.
    :param msgIds: 3-tuple of (start, stop, step), for xrange
    """
    msgsArchived = 0
    serverErrors = 0
    waitTime = datetime.timedelta(0)
    for x in xrange(*msgIds):
      if x in self.archived:
        continue  # Already have x. For retry modes.
      if waitTime:
        self.logger.info("Sleeping for %s...", waitTime)
        time.sleep(waitTime.total_seconds())
      self.logger.info("Attempting to archive message %d...", x)
      http_code = self.archive_message(x)
      if http_code >= 500:
        serverErrors += 1
        if serverErrors >= maxServerErrors:
          raise Exception('Too many server errors: %d' % serverErrors)
      else:
        serverErrors = 0
      if http_code == 200:
        waitTime = minWaitTime
        self.archived.add(x)
        msgsArchived += 1
      elif http_code == 404:  # Expect lots of holes in message ids
        waitTime = minWaitTime
        self.logger.warn("Message %d not found", x)
      else:
        if waitTime:
          waitTime *= 2
          if waitTime > maxWaitTime:
            waitTime = maxWaitTime
            # Try creating a new session, as this reconnects.
            #self.http = requests.Session()
        else:
          waitTime = minWaitTime
        self.logger.error("Message %d got unexpected HTTP error code %d",
                            x, http_code)
    return msgsArchived


  def archive_message(self, msgNumber):
    resp = self.http.get(
      'https://groups.yahoo.com/api/v1/groups/%s/messages/%d/raw' % (
      self.groupName, msgNumber), headers=headers, cookies=cookies)
    if resp.status_code != 200:
      return resp.status_code
    # Use postDate to create subdirs if it's available.
    writeDir = self.groupName
    try:
      # Write to groupName/year/month/msgid.json if postDate is available.
      # Otherwise, use groupName/msgid.json.
      postDate = datetime.datetime.fromtimestamp(
        int(json.loads(resp.text)['ygData']['postDate']))
      writeDir = os.path.join(
        self.groupName, postDate.strftime('%Y'), postDate.strftime('%m'))
      if not os.path.exists(writeDir):
        os.makedirs(writeDir)
    except:
      self.logger.warn(("Not using date based path because postDate could " +
                        "not be parsed: %s"), sys.exc_info()[1])

    # Write file with atomic rename to .json file to ensure .json files are never
    # corrupted by partial writes (caused by interruptions and crashes).
    jsonPath = os.path.join(writeDir, "%d.json" % msgNumber)
    self.logger.info("Writing message %d to %s", msgNumber, jsonPath)
    tmpPath = "%s.tmp" % jsonPath
    with open(tmpPath, "wb") as writeFile:
      writeFile.write(resp.text.encode('utf-8'))
    os.rename(tmpPath, jsonPath)
    return resp.status_code


if __name__ == "__main__":
  nologs = False
  if "nologs" in sys.argv:
    nologs = True
    sys.stderr.write("Logging mode OFF\n")
    sys.argv.remove("nologs")
  if len(sys.argv) < 2:
    sys.stderr.write("Usage: %s <groupName0,groupName1,...> [<mode>] [nologs]\n" %
                       __file__)
    sys.exit(4)
  groupNames = sys.argv[1].split(",")
  if nologs:
    h = logging.NullHandler()
  else:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter(
      '[%(levelname)s %(asctime)s %(name)s] %(message)s'))
  if len(sys.argv) > 2:
    mode = sys.argv[2]
  else:
    mode = "update"
  # Reuse the same connection for every request. This is faster and avoids us
  # being spammed by Yahoo.
  session = requests.Session()
  for groupName in groupNames:
    archiver = GroupArchiver(groupName, h, session)
    archiver.archive_group(mode)
    session = archiver.http  # In case archiver created a new session.
