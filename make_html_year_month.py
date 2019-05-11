#!/usr/bin/env python3
"""
Scan the given directory for JSON and HTML files named <messageID>.json or
<messageID>.html. Create HTML files from YahooGroups JSON and organize them into
year/month directories according to the post date. Copy existing HTML files into
the same year/month directories.
"""

import datetime
import dateutil.parser
import email
import html
import io
import json
import os
import pytz
import re
import sys
import traceback


# Groups: 1: message ID
JSON_MATCH = re.compile(r'(\d+)\.json$', re.IGNORECASE)

# Groups: 1: message ID
HTML_MATCH = re.compile(r'(\d+)\.html?$', re.IGNORECASE)

BR_SEARCH = re.compile(rb'<br(?:\s|/)*>', re.IGNORECASE)
TAGS_SEARCH = re.compile(rb'<[^>]*>')

# Groups: 1: date header value
DATE_SEARCH = re.compile(rb'Date:\s*(.+)$', re.IGNORECASE)


class Archiver(object):
  def __init__(self, srcdir, dstdir):
    self._srcdir = srcdir
    self._dstdir = dstdir

  def Archive(self):
    for dirpath, _, filenames in os.walk(self._srcdir, followlinks=True):
      for basename in filenames:
        stubname = None
        func = None
        json_match = JSON_MATCH.match(basename)
        if json_match:
          stubname = json_match.group(1)
          func = self._ArchiveJson
        else:
          html_match = HTML_MATCH.match(basename)
          if html_match:
            stubname = html_match.group(1)
            func = self._ArchiveHtml
        if not stubname:
          continue
        path = os.path.join(dirpath, basename)
        try:
          func(path, stubname)
        except:
          sys.stderr.write('Failed to archive %s: %s\n' % (path, traceback.format_exc()))

  def _ArchiveJson(self, srcpath, stubname):
    with open(srcpath, 'rb') as fh:
      srcpath_json = json.load(fh)
    yg_data = srcpath_json['ygData']
    raw_email = yg_data['rawEmail']
    post_date = None
    try:
      post_date = datetime.datetime.fromtimestamp(int(yg_data['postDate']))
    except:
      sys.stderr.write('Failed to parse date from %s JSON postDate: %s\n' % (
        srcpath, traceback.format_exc()))
    if post_date is None:
      try:
        post_date = self._ParseDate(html.unescape(raw_email), srcpath)
      except:
        sys.stderr.write('Failed to parse date from %s: %s\n' % (
          srcpath, traceback.format_exc()))
    if not post_date:
      sys.stderr.write('No valid date found for %s\n' % srcpath)
    dstpath = self._CheckDstPath(post_date, stubname)
    if not dstpath:
      return False
    with open(dstpath, 'wb') as fh:
      fh.write(b'<html><head></head><body><pre>\n%s\n</pre></body></html>\n' % 
                 raw_email.encode('utf-8'))
    return True

  def _ArchiveHtml(self, srcpath, stubname):
    with open(srcpath, 'rb') as fh:
      html_str = fh.read()
    post_date = None
    try:
      post_date = self._ParseDate(html_str, srcpath)
    except:
      sys.stderr.write('Failed to parse date from %s: %s\n' % (
        srcpath, traceback.format_exc()))
    if not post_date:
      sys.stderr.write('No valid date found for %s\n' % srcpath)
    dstpath = self._CheckDstPath(post_date, stubname)
    if not dstpath:
      return False
    with open(dstpath, 'wb') as fh:
      fh.write(html_str)
    return True

  def _CheckDstPath(self, post_date, stubname):
    """Compute destination path given date and stubname. Create parent directories
    if needed. Return the path or None. post_date may be None to use self._dstdir."""
    dstdir = self._DstDir(post_date)
    os.makedirs(dstdir, exist_ok=True)
    dstpath = os.path.join(dstdir, '%s.html' % stubname)
    if os.path.exists(dstpath):
      sys.stderr.write('Refusing to clobber existing file at %s\n' % dstpath)
      return None
    return dstpath

  def _ParseDate(self, html_str, srcpath):
    for line in io.BytesIO(TAGS_SEARCH.sub(b'', BR_SEARCH.sub(b'\n', html_str))):
      m = DATE_SEARCH.search(line)
      if not m:
        continue
      try:
        post_date = dateutil.parser.parse(m.group(1)).astimezone(pytz.utc)
        return post_date
      except:
        pass
    return None
    
  def _ParseDateYG(self, raw_email_str):
    """Parse datetime from an HTML-escaped email string (YahooGroups format)."""
    dateutil.parser.parse(
      email.message_from_string(
        html.unescape(raw_email_str))['date']).astimezone(pytz.utc)

  def _DstDir(self, post_date):
    if post_date is None:
      return self._dstdir
    else:
      return os.path.join(self._dstdir, post_date.strftime('%Y'), post_date.strftime('%m'))


def main(argv):
  if len(argv) < 2:
    sys.stderr.write('Usage: %s <directory>\n' % __file__)
    sys.exit(1)
  srcdir = sys.argv[1]
  # Don't create output dir as subdir of input dir to avoid rereading our own
  # output files.
  htmldir = '%s_html' % srcdir
  archiver = Archiver(srcdir, htmldir)
  archiver.Archive()


if __name__ == "__main__":
  main(sys.argv)
