#!/usr/bin/env python
"""
Yahoo-Groups-Archiver Copyright 2015, 2017, 2018 Andrew Ferguson and others

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
"""

cookie_T = "COOKIE_T_DATA_GOES_HERE"
cookie_Y = "COOKIE_Y_DATA_GOES_HERE"

# Set this to False if you do not want to write logs out to a "groupName.txt" file
writeLogFile = True

# Set this to False if you do not want to archive attachments
saveAttachments = True

import json  # required for reading various JSON attributes from the content
import requests  # required for fetching the raw messages
import os  # required for checking if a file exists locally
import time  # required if Yahoo blocks access temporarily (to wait)
import sys  # required to cancel script if blocked by Yahoo
import shutil  # required for deletung an old folder
import glob  # required to find the most recent message downloaded
import time  # required to log the date and time of run
import re  # required to parse messages to identify/download attachments


def json_path(groupName, msgNumber):
    """ Return the path to the json file for a given group/message """
    return os.path.join(groupName, "{}.json".format(msgNumber))


def attachment_path(groupName, msgNumber, attachment):
    """ Return the path to an attachment for given group/message """
    return os.path.join(groupName, "{}-{}".format(msgNumber, attachment))


def is_valid_file(path):
    return os.path.isfile(path) and os.path.getsize(path) > 0


def log(msg, groupName):
    print(msg)
    if writeLogFile:
        logF = open(groupName + ".txt", "a")
        logF.write("\n" + msg)
        logF.close()


def exit_blocked(groupName):
    # we are most likely being blocked by Yahoo
    log("Archive halted - it appears Yahoo has blocked you.", groupName)
    log(
        "Check if you can access the group's homepage from your "
        "browser. If you can't, you have been blocked.",
        groupName,
    )
    log(
        "Don't worry, in a few hours (normally less than 3) you'll "
        "be unblocked and you can run this script again - it'll "
        "continue where you left off.",
        groupName,
    )
    sys.exit()


def archive_group(groupName, mode="update"):
    log(
        "\nArchiving group '"
        + groupName
        + "', mode: "
        + mode
        + " , on "
        + time.strftime("%c"),
        groupName,
    )
    startTime = time.time()
    msgsArchived = 0
    if mode == "retry":
        # don't archive any messages we already have
        # but try to archive ones that we don't, and may have
        # already attempted to archive
        min = 1
    elif mode == "update":
        # start archiving at the last+1 message message we archived
        mostRecent = 1
        if os.path.exists(groupName):
            oldDir = os.getcwd()
            os.chdir(groupName)
            for file in glob.glob("*.json"):
                if int(file[0:-5]) > mostRecent:
                    mostRecent = int(file[0:-5])
            os.chdir(oldDir)

        min = mostRecent
    elif mode == "restart":
        # delete all previous archival attempts and archive everything again
        if os.path.exists(groupName):
            shutil.rmtree(groupName)
        min = 1

    else:
        print(
            "You have specified an invalid mode (" + mode + ").\n"
            "Valid modes are:\n"
            "update - add any new messages to the archive\n"
            "retry - attempt to get all messages that are not in the archive\n"
            "restart - delete archive and start from scratch"
        )
        sys.exit()

    if not os.path.exists(groupName):
        os.makedirs(groupName)
    max = group_messages_max(groupName)
    for x in range(min, max + 1):
        if not is_valid_file(json_path(groupName, x)):
            print("Archiving message " + str(x) + " of " + str(max))
            success = archive_message(groupName, x)
            if success == True:
                msgsArchived = msgsArchived + 1

    log(
        "Archive finished, archived "
        + str(msgsArchived)
        + ", time taken is "
        + str(time.time() - startTime)
        + " seconds",
        groupName,
    )


def group_messages_max(groupName):
    s = requests.Session()
    resp = s.get(
        "https://groups.yahoo.com/api/v1/groups/"
        + groupName
        + "/messages?count=1&sortOrder=desc&direction=-1",
        cookies={"T": cookie_T, "Y": cookie_Y},
    )
    try:
        pageHTML = resp.text
        pageJson = json.loads(pageHTML)
        return pageJson["ygData"]["totalRecords"]
    except ValueError:
        # "Stay signed in" and "Trouble signing in" are no longer in pageHTML,
        # or include odd unicode encodings instead of spaces that this can't
        # easily match.  Just print this error no matter what.
        print(
            "Unexpected error getting message count.\n"
            "The group you are trying to archive is a private group. To archive\n"
            "a private group using this tool, login to a Yahoo account that has\n"
            "access to the private groups, then extract the data from the\n"
            "cookies Y and T from the domain yahoo.com . Paste this data into\n"
            "the appropriate variables (cookie_Y and cookie_T) at the top of\n"
            "this script, and run the script again."
        )
        sys.exit()


def make_request(groupName, url, max_retries=3, **kwargs):
    if "cookies" not in kwargs:
        kwargs["cookies"] = {"T": cookie_T, "Y": cookie_Y}
    if "allow_redirects" not in kwargs:
        kwargs["allow_redirects"] = True

    s = requests.Session()
    attempt = 1
    while True:
        resp = s.get(url, **kwargs)
        if resp.status_code == 200:
            if attempt > 1:
                print("Success on attempt {} of {}".format(attempt, max_retries))
            # Success!
            break
        elif resp.status_code == 500:
            # No point in looping more. We are most likely being blocked by Yahoo.
            exit_blocked(groupName)
        elif attempt > max_retries or resp.status_code in (404,):
            # Unrecoverable error or max retries hit.  Time to leave no matter what.
            log(
                "Failed after attempt {} of {} for url {} (status: {})".format(
                    attempt, max_retries, url, resp.status_code
                ),
                groupName,
            )
            break
        print(
            "Retrying after attempt {} of {} for url {} (status: {})".format(
                attempt, max_retries, url, resp.status_code
            )
        )
        time.sleep(attempt ^ 2)  # Sleep for an incremental backoff
        attempt += 1

    return resp


def archive_attachments(groupName, msgNumber):
    # First, grab the URL that the web user interface uses to get the HTML page content.
    # This contains links to downloadable attachments.
    msgUrl = "https://groups.yahoo.com/neo/groups/{}/conversations/messages/{}?noNavbar=true&chrome=raw".format(
        groupName, msgNumber
    )
    resp = make_request(groupName, msgUrl)
    if resp.status_code != 200:
        return False
    data = json.loads(resp.text)
    html = data["html"]

    # Loop through any anchor tags that match the appropriate patterns.
    href_pat = re.compile(r'href="(https://xa.yimg.com/kq/groups/.+?\?download=1)"')
    filename_pat = re.compile(r'label="Download attachment (.+?)"')
    anchors = re.findall(re.compile(r"<a\s(.+?)>"), html)
    for a in anchors:
        m = href_pat.search(a)
        if not m:
            continue
        url = m.group(1)
        m = filename_pat.search(a)
        if not m:
            continue
        filename = m.group(1)
        # print("Found: {}:\n  {}".format(filename, url))

        # Save the attachment if we don't already have it.
        savePath = attachment_path(groupName, msgNumber, filename)
        if is_valid_file(savePath):
            print("Attachment {} already exists.".format(savePath))
        else:
            r = make_request(groupName, url, headers={"referer": msgUrl})
            if r.statusCode == 200:
                with open(savePath, "wb") as f:
                    f.write(r.content)
                    print("Saved attachment: {}".format(savePath))


def archive_message(groupName, msgNumber):
    if saveAttachments:
        archive_attachments(groupName, msgNumber)

    resp = make_request(
        groupName,
        "https://groups.yahoo.com/api/v1/groups/{}/messages/{}/raw".format(
            groupName, msgNumber
        ),
    )
    if resp.status_code != 200:
        return False

    msgJson = resp.text
    writeFile = open(json_path(groupName, msgNumber), "wb")
    writeFile.write(msgJson.encode("utf-8"))
    writeFile.close()
    return True


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if "nologs" in sys.argv:
        print("Logging mode OFF")
        writeLogFile = False
        sys.argv.remove("nologs")
    if len(sys.argv) > 2:
        archive_group(sys.argv[1], sys.argv[2])
    else:
        archive_group(sys.argv[1])
