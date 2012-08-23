#!/usr/bin/python

# -*- coding: ascii -*-

# Reads in nagios config, dumps out command that nagios is running
# Mike Lindsey (mike@5dninja.net) 6/5/2008


import base64
import os
import socket
import sys
import traceback
import time
import re
import simplejson

from optparse import OptionParser

fileroot = '/usr/local/nagios/etc/'
resourcefiles = ('resource.cfg')

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
    parser.add_option("-s", "--statc", type="string", dest="statc",
                            help="Server stats client.", \
                            default="/usr/local/nagios/bin/nagiosstatc")
    parser.add_option("-n", "--nopw", action="store_true", dest="nopw",
                            default=False,
                            help="do not expand nagios resource macros")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                            default=False,
                            help="print debug messages to stderr")
    (options, args) = parser.parse_args()
    exitflag = 0
    if not options.host:
        exitflag = exitflag + 1
        print "Need host"
    if not options.service:
        exitflag = exitflag + 1
        print "Need service"
    elif ' ' in options.service:
        options.service = options.service.replace(' ', '\\ ')
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

def get_host():
    """Query stats server for hostname data"""
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")
    cmd = '%s -q "object host %s address"' % (options.statc, options.host)
    if options.verbose:
        print "Querying with '%s'" % (cmd)
    address = os.popen(cmd).read()
    address = simplejson.loads(address)
    if not address.has_key('query_ok'):
        print "Unable to query stats server for command data."
        sys.exit(0)
    if not address['query_ok']:
        print "Host not found in nagios config."
        sys.exit(0)
    else:
        address = address['address']
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")
    return address

def get_service():
    """Query stats server for commandline data"""
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")
    cmd = '%s -q "object service %s %s check_command"' % \
            (options.statc, options.host, options.service)
    if options.verbose:
        print "Querying with '%s'" % (cmd)
    check_command = os.popen(cmd).read()
    check_command = simplejson.loads(check_command)
    if not check_command.has_key('query_ok'):
        print "Unable to query stats server for command data."
        sys.exit(0)
    if not check_command['query_ok']:
        print "No such service found."
        sys.exit(0)
    else:
        check_command = check_command['check_command']

    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")
    return check_command

def get_resources():
    """We have to hit local disk for resource macros, as
    they contain passwords, and should not be made available
    over the stats socket."""
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() +
                            "()\n")
    resources = ''
    f = open(fileroot + resourcefiles)
    resources = resources + f.read()

    resourcelist  = resources.split('$USER')
    resourceclean = {}

    i = 0
    for resource in resourcelist:
        if i > 0:
            arg = resourcelist[i].split('$')[0]
            value = resourcelist[i].split('=')[1].split('\n')[0]
            resourceclean[arg] = value
        i = i + 1

    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() +
                            "()\n")
    return resourceclean

def get_command():
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")
    cmd = '%s -q "object command %s"' % \
            (options.statc, check_command.split('!')[0])
    if options.verbose:
        print "Querying with '%s'" % (cmd)
    command = os.popen(cmd).read()
    command = simplejson.loads(command)
    if not command.has_key('query_ok'):
        print "Unable to query stats server for command-line data."
        sys.exit(0)
    if not command['query_ok']:
        print "No such service found."
        sys.exit(0)
    else:
        command = command['command_line']

    command = command.split()
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")
    return command

def expand_macros(bit):
    if options.verbose: sys.stderr.write(">>DEBUG start - " + funcname() + 
                            "()\n")

    cmd = '%s -q "object service %s %s"' % (options.statc, options.host, options.service)
    if options.verbose:
        print "Querying with '%s'" % (cmd)
    svchash = os.popen(cmd).read()
    svchash = simplejson.loads(svchash)
    if bit[-4] == 'CRIT':
        os.environ['NAGIOS__SERVICECRIT'] = svchash['_CRIT']
        os.environ['NAGIOS__SERVICECRITTHRESH'] = svchash['_CRITTHRESH']
    elif bit[-4] == 'WARN':
        os.environ['NAGIOS__SERVICEWARN'] = svchash['_WARN']
        os.environ['NAGIOS__SERVICEWARNTHRESH'] = svchash['_WARNTHRESH']
        
    # now that we've grabbed and set the env macros, expand it!
    cmd = 'bash -c "echo %s"' % (bit)
    bit = os.popen(cmd).read().rstrip()
        
    if options.verbose: sys.stderr.write(">>DEBUG end    - " + funcname() + 
                            "()\n")
    return bit

if __name__ == '__main__':
    options = init()

    address = get_host()
    check_command = get_service()
    fullcommand = get_command()
    resources = get_resources()

    check_command = check_command.split('!')
    buildcommand = ''
    for bit in fullcommand:
        if bit.startswith('$NAGIOS__SERVICE'):
            bit = expand_macros(bit)
        buildcommand = buildcommand + " " + bit

    i = 0
    for check in check_command:
        if i > 0:
            p = re.compile('\$ARG' + str(i) + '\$')
            buildcommand = p.sub(check_command[i], buildcommand)
        i = i + 1

    p = re.compile('\$HOSTADDRESS\$')
    buildcommand = p.sub(address, buildcommand)
    buildcommand = p.sub(address, buildcommand)
    p = re.compile('\$HOSTNAME\$')
    buildcommand = p.sub(options.host, buildcommand)
    buildcommand = p.sub(options.host, buildcommand)
    x = 1
    p = re.search('\$USER', buildcommand)
    if p == None or options.nopw:
        finalcommand = buildcommand
    else:
        userlist = buildcommand.split('$USER')
        i = 0
        finalcommand = ''
        for user in userlist:
            if i == 0:
                finalcommand = finalcommand + user
            if i > 0:
                arg = user.split('$')
                #print "Substituting USER", arg[0], resources[arg[0]]
                finalcommand = finalcommand + resources[arg[0]]
                finalcommand = finalcommand + arg[1]
            i = i + 1

    print finalcommand[1:]
