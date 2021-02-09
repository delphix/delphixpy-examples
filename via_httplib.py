#!/usr/bin/env python

#
# Copyright (c) 2018 by Delphix. All rights reserved.
#

from __future__ import print_function

import argparse
import httplib
import json
import os
import sys
import urllib
from argparse import RawTextHelpFormatter

SCRIPT_DESCRIPTION = """
Connect to Delphix engine to run some queries using the http lib library
"""

# globals used by helper functions
dlpx_host = ""
dlpx_user = ""
dlpx_password = ""
dlpx_cookie = None
major = 1  # API Major version number
minor = 6  # API Minor version number
micro = 0  # API micro version number


def main():
    global dlpx_host
    global dlpx_user
    global dlpx_password
    global dlpx_cookie

    # parse args and print usage message if necessary
    parser = argparse.ArgumentParser(
        description=SCRIPT_DESCRIPTION, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument("dlpxHost", help="The target Delphix Engine.", type=str)
    parser.add_argument(
        "dlpxUser",
        help="The username to use to log into the Delphix Engine.",
        type=str,
        nargs="?",
        default="delphix_admin",
    )
    parser.add_argument(
        "dlpxPassword",
        help="The password to use to log into the Delphix Engine.",
        type=str,
        nargs="?",
        default="delphix",
    )
    args = parser.parse_args()

    # save args to variables with shorter names
    dlpx_host = args.dlpxHost
    dlpx_user = args.dlpxUser
    dlpx_password = args.dlpxPassword

    api_version = {"type": "APIVersion", "major": major, "minor": minor, "micro": micro}

    # log into the Delphix Engine in order to set cookie
    print("Logging into " + dlpx_host + "...")
    log_into_dlpx_engine(api_version)
    print("SUCCESS - Logged in as " + dlpx_user)

    response = dlpx_get("delphix/user")
    for item in response["result"]:
        print(item["name"])
    # exit with success
    sys.exit(0)


def check_response(response):
    if response.status is not 200:
        sys.stderr.write(
            "ERROR: Expected a response of HTTP status 200 (Success) but received something different.\n"
        )
        sys.stderr.write("Response status: " + str(response.status) + "\n")
        sys.stderr.write("Response reason: " + response.reason + "\n")
        sys.exit(1)


def dlpx_post_json(resource, payload):
    global dlpx_host
    global dlpx_user
    global dlpx_password
    global dlpx_cookie

    # encode payload for request
    data = json.dumps(payload)

    # form http header, add cookie if one has been set
    headers = {"Content-type": "application/json"}
    if dlpx_cookie is not None:
        headers["Cookie"] = dlpx_cookie

    # issue request
    h = httplib.HTTPConnection(dlpx_host)
    h.request("POST", "/resources/json/" + resource, data, headers)
    r = h.getresponse()
    check_response(r)

    # save cookie if one was received
    if r.getheader("set-cookie", None) is not None:
        dlpx_cookie = r.getheader("set-cookie")

    # return response as parsed json
    r_payload = r.read()
    return json.loads(r_payload)


def dlpx_get(resource, payload=None):
    global dlpx_host
    global dlpx_user
    global dlpx_password
    global dlpx_cookie

    if payload:
        # encode payload for request
        data = json.dumps(payload)
    else:
        data = None

    # form http header, add cookie if one has been set
    headers = {"Content-type": "application/json"}
    if dlpx_cookie is not None:
        headers["Cookie"] = dlpx_cookie

    # issue request
    h = httplib.HTTPConnection(dlpx_host)
    h.request("GET", "/resources/json/" + resource, data, headers)
    r = h.getresponse()
    check_response(r)

    # save cookie if one was received
    if r.getheader("set-cookie", None) is not None:
        dlpx_cookie = r.getheader("set-cookie")

    # return response as parsed json
    r_payload = r.read()
    return json.loads(r_payload)


def log_into_dlpx_engine(api_version):
    dlpx_post_json(
        "delphix/session",
        {
            "type": "APISession",
            "version": {
                "type": "APIVersion",
                "major": api_version["major"],
                "minor": api_version["minor"],
                "micro": api_version["micro"],
            },
        },
    )

    dlpx_post_json(
        "delphix/login",
        {"type": "LoginRequest", "username": dlpx_user, "password": dlpx_password},
    )


if __name__ == "__main__":
    main()
