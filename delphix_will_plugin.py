#!/usr/bin/env python
# For use with HipChat and Will
# https://github.com/skoczen/will

import imp
import shlex
import subprocess

from will.decorators import hear
from will.decorators import periodic
from will.decorators import randomly
from will.decorators import rendered_template
from will.decorators import require_settings
from will.decorators import respond_to
from will.decorators import route
from will.plugin import WillPlugin

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.web import database

VERSION = 0.001


class DelphixPlugin(WillPlugin):
    @respond_to("listvdbs")
    def list_databases_will(self, message):
        foo = imp.load_source(
            "list_all_databases", "delphixpy-examples/list_all_databases.py"
        )
        vdblist = "\n".join(each.name for each in foo.all_databases)
        will_response = (
            "There are "
            + str(len(foo.all_databases))
            + " databases in the LandsharkEngine\n"
            + vdblist
        )
        self.reply(message, will_response)

    @respond_to("snapshot (?P<v_object>.*)")
    def snapshot_databases_will(self, message, v_object=None):
        if " in " not in v_object:
            will_response = (
                "Please specify group with request. For example:\n"
                "snapshot Employee Oracle 11G DB in Sources"
            )
            self.reply(message, will_response)
        else:
            v_object = v_object.split(" in ", 1)
            vdb_name = v_object[0]
            vdb_group = v_object[1]
            self.reply(
                message,
                "Snapping " + vdb_name + ". Will let you know when it is complete.",
            )
            p = subprocess.Popen(
                [
                    "python",
                    "delphixpy-examples/dx_snapshot_db.py",
                    "--group",
                    vdb_group,
                    "--name",
                    vdb_name,
                    "--config",
                    "delphixpy-examples/dxtools.conf",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.reply(message, vdb_name + " Snapshot Complete\n" + p.stdout.read())

    @respond_to("provision vdb (?P<v_object>.*)")
    def provision_databases_will(self, message, v_object=None):
        provision_parameters = shlex.split(
            "python delphixpy-examples/dx_provision_vdb.py --config delphixpy-examples/dxtools.conf "
            + v_object
        )
        self.reply(message, str(provision_parameters))
        self.reply(message, "Executing provision job")
        p = subprocess.Popen(
            provision_parameters, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        self.reply(message, "Provision Request Complete\n" + p.stdout.read())

    @respond_to("delete vdb (?P<v_object>.*)")
    def delete_databases_will(self, message, v_object=None):
        if " in " not in v_object:
            will_response = (
                "Please specify group with request. For example:\n"
                "delete Employee Oracle 11G DB in Sources"
            )
            self.reply(message, will_response)
        else:
            v_object = v_object.split(" in ", 1)
            vdb_name = v_object[0]
            vdb_group = v_object[1]
            self.reply(
                message,
                "Deleting " + vdb_name + ". Will let you know when it is complete.",
            )
            p = subprocess.Popen(
                [
                    "python",
                    "delphixpy-examples/dx_delete_vdb.py",
                    "--group",
                    vdb_group,
                    "--name",
                    vdb_name,
                    "--config",
                    "delphixpy-examples/dxtools.conf",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.reply(message, vdb_name + " Delete Complete\n" + p.stdout.read())

    @respond_to("refresh vdb (?P<v_object>.*)")
    def refresh_vdbs_will(self, message, v_object=None):
        if " in " not in v_object:
            will_response = (
                "Please specify group with request. For example:\n"
                "refresh autoprod in Analytics"
            )
            self.reply(message, will_response)
        else:
            v_object = v_object.split(" in ", 1)
            vdb_name = v_object[0]
            vdb_group = v_object[1]
            self.reply(
                message,
                "Refreshing " + vdb_name + ". Will let you know when it is complete.",
            )
            p = subprocess.Popen(
                [
                    "python",
                    "delphixpy-examples/dx_refresh_db.py",
                    "--group",
                    vdb_group,
                    "--name",
                    vdb_name,
                    "--config",
                    "delphixpy-examples/dxtools.conf",
                    "--timestamp",
                    "@2016-10-14T20:55:05.995Z",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.reply(message, vdb_name + " Refresh Complete\n" + p.stdout.read())

    @respond_to("refresh jetstream (?P<v_object>.*)")
    def refresh_jetstream_will(self, message, v_object=None):
        if " in " not in v_object:
            will_response = (
                "Please specify group with request. For example:\n"
                "refresh jetstream Sugar Automated Testing Container in"
                " Masked SugarCRM Application"
            )
            self.reply(message, will_response)
        else:
            v_object = v_object.split(" in ", 1)
            container_name = v_object[0]
            container_template = v_object[1]
            self.reply(
                message,
                "Refreshing Jetstream Container: "
                + container_name
                + ". Will let you know when it is complete.",
            )
            p = subprocess.Popen(
                [
                    "python",
                    "delphixpy-examples/dx_jetstream_container.py",
                    "--operation",
                    "refresh",
                    "--template",
                    container_template,
                    "--container",
                    container_name,
                    "--config",
                    "delphixpy-examples/dxtools.conf",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.reply(
                message, container_name + " Refresh Complete\n" + p.stdout.read()
            )

    @respond_to("bonjour")
    def say_bonjour_will(self, message):
        """bonjour: Landshark parles the Francais!"""
        self.reply(
            message, "bonjour! Je m'appelle Landshark! Je suis pret a travailler!"
        )
