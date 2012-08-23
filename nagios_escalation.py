#!/usr/bin/python

# -*- coding: ascii -*-

# Reads in nagios config, dumps out escalation that nagios is using.
# Mike Lindsey (mike@5dninja.net) 12/9/2009


import base64
import os
import socket
import sys
import traceback
import time
import re

from optparse import OptionParser

fileroot = '/usr/local/nagios/etc/'

def funcname():
    # so we don't have to keep doing this over and over again.
    return sys._getframe(1).f_code.co_name

def init():
    # collect option information, display help text if needed, set up debugging
    parser = OptionParser()
    parser.add_option("-H", "--Host", type="string", dest="host",
                            help="hostname")
    parser.add_option("-S", "--Service", type="string", dest="service",
                            help="service")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                            default=False,
                            help="print debug messages to stderr")
    (options, args) = parser.parse_args()
    exitflag = 0
    if not options.host:
        exitflag = exitflag + 1
        print "Need host"
    if exitflag > 0:
        parser.print_help()
        sys.exit(0)
    if options.verbose: sys.stderr.write(">>DEBUG sys.argv[0] running in " +
                            "debug mode\n")
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")

    return options

def get_escalations():
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")
    if options.service:
        escalationfile = ('service_escalations.cfg')
    else:
        escalationfile = ('host_escalations.cfg')

    f = open(fileroot + escalationfile)
    escalations = str(f.read())

    escalationlist = escalations.split('define')
    i = 0
    for escalationL in escalationlist:
        escalation = escalationL.split()
        if options.host in escalation or options.host in escalationL:
            if options.service and options.service in escalation or '*' in escalation:
                print "%s:%s" % (escalation[3], escalation[-2])
            elif not options.service:
                print "%s:%s" % (escalation[3], escalation[-2])
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")
    return 


options = init()

get_escalations()

