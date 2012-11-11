#!/usr/bin/env python

# -*- coding: ascii -*-

# ack alerts from command line

import base64
import os
import socket
import sys
import traceback
import time
import MySQLdb

from optparse import OptionParser

def funcname():
    # so we don't have to keep doing this over and over again.
    return sys._getframe(1).f_code.co_name

def init():
    # collect option information, display help text if needed, set up debugging
    parser = OptionParser()
    parser.add_option("-O", "--object", type="string", dest="object",
                            help="""Parse object.cache file for host/service validity.
                            If blank, no validation is made.
                            default: /usr/local/nagios/var/objects.cache""",
                            default='/usr/local/nagios/var/objects.cache')
    parser.add_option("-C", "--cmdspool", type="string", dest="cmdspool",
                            help="""Location of cmd spool.
                            default: /usr/local/nagios/var/rw/nagios.cmd.""",
                            default='/usr/local/nagios/var/rw/nagios.cmd')
    parser.add_option("-H", "--hostname", type="string",
                            help="Hostname to ack. [required]")
    parser.add_option("-S", "--service", type="string",
                            help="Service name.  Leave off if acking host")
    parser.add_option("-c", "--comment", type="string",
                            help="Ack comment in quotes. [required]")
    parser.add_option("-u", "--username", type="string",
                            help="Your username here. [required]")
    parser.add_option("-s", "--sticky", type="int", dest="sticky", default=1,
                            help="Make ack sticky, default: 1")
    parser.add_option("-N", "--notify", type="int", dest="notify", default=0,
                            help="Notify of ACK, default: 0")
    parser.add_option("-P", "--persist", type="int", dest="persist", default=1,
                            help="Persist comment over nagios restarts, defaut: 1")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                            help="print debug messages to stderr")
    (options, args) = parser.parse_args()
    if options.verbose: sys.stderr.write(">>DEBUG sys.argv[0] running in debug mode\n")
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + "()\n")
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + "()\n")
    exit = 0
    if not options.hostname:
        sys.stderr.write("No hostname entered\n")
        exit = 1
    if not options.comment:
        sys.stderr.write("No comment entered\n")
        exit = 1
    if not options.username:
        sys.stderr.write("No username entered\n")
        exit = 1
    if exit == 1:
        parser.print_help()
        sys.exit(3)
    return options

def process_object(file):
    """Read object file, sanitize and store in global variable."""
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() +
                            "()\n")
    object = {}
    try:
        object_lines = open(file).readlines()
    except:
        print("Tried to open cache file - failed")
        print("Pass valid -O, or -O \"\"")
        sys.exit(3)
    else:
        tag = ''
        last = ''
        for line in object_lines:
            line = line.rstrip().lstrip()
            if not line:
                continue
            if line.startswith('#'):
                if options.verbose and last != 'comment':
                    last = 'comment'
                continue
            last = ''
            # Here there be data!
            if line.startswith('define'):
                temp = {}
                old_tag = tag
                tag = line.split()[1]
                if tag.endswith('status'):
                    tag = tag.split('status')[0]
                if tag != old_tag:
                    tag_count = 1
                else:
                    tag_count += 1
                if not object.has_key(tag):
                    object[tag] = {}
                continue
            # Here ends data!
            if line.endswith('}'):
                if line != '}':
                    (entry, value) = line.split('\t', 1)
                    try:
                        value = float(value)
                    except:
                        value = value.strip()
                    temp[entry] = value
                elif temp.has_key('%s_name' % (tag)):
                    # Catchall for anything with *_name
                    tag_name = temp.pop('%s_name' % (tag))
                    object[tag][tag_name] = temp
                elif temp.has_key('service_description'):
                    # Services 
                    if not temp.has_key('host_name'):
                        temp['host_name'] = None
                    host_name = temp.pop('host_name')
                    if not object[tag].has_key(host_name):
                        object[tag][host_name] = {}
                    service = temp.pop('service_description')
                    object[tag][host_name][service] = \
                            temp
            elif line:
                (entry, value) = line.split('\t', 1)
                try:
                    value = float(value)
                except:
                    value = value.strip()
                temp[entry] = value
    if options.verbose: sys.stderr.write(">>DEBUG end   - " + funcname() +
                            "()\n")
    return object

def check_inputs(object):
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() +
                            "()\n")
    exit = 0
    if options.hostname not in object['host']:
        print "Error no such host, '%s'" % (options.hostname)
        exit = 1

    if not exit and options.service not in object['service'][options.hostname]:
        print "Error, no such service on '%s'" % (options.hostname)
        print object['service'][options.hostname].keys()
        exit = 1

    if exit: sys.exit(0)

    if options.verbose: sys.stderr.write(">>DEBUG end   - " + funcname() +
                            "()\n")


if __name__ == '__main__':
    options = init()
    if options.object not in ["", None]:
        check_inputs(process_object(options.object))

    try:
        spool = open(options.cmdspool, 'a')
    except:
        print "Spool file access error.  Perhaps you're not using sudo?"
        sys.exit(3)

    if not options.service:
        print >> spool, ('[%i] ACKNOWLEDGE_HOST_PROBLEM;%s;%i;%i;%i;%s;%s') % \
                (int(time.time()), options.hostname, options.sticky, options.notify,
                        options.persist, options.username, options.comment)

    else:
        print >> spool, ('[%i] ACKNOWLEDGE_SVC_PROBLEM;%s;%s;%i;%i;%i;%s;%s') % \
                (int(time.time()), options.hostname, options.service, options.sticky, 
                        options.notify, options.persist, options.username, options.comment)

    spool.close()
