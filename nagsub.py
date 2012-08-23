#!/usr/bin/python -u
# -*- coding: ascii -*-
"""Script for submitting nagios events or notification data to a CMDB.
    Event or Notification handler output is fed to the script via STDIN.
    All other variables are gleaned from the environment."""

import urllib
import urllib2
import pprint
import subprocess
import shlex
import fcntl
import time
import sys
import os
import datetime

import asdb

from optparse import OptionParser

def funcname(enter=True, forceverbose=False):
    """Display function name of parent function"""
    try:
        if forceverbose or options.verbose:
            if enter:
                sys.stdout.write(">>DEBUG start - %s()\n" % (sys._getframe(1).f_code.co_name))
            else:
                sys.stdout.write(">>DEBUG end   - %s()\n" % (sys._getframe(1).f_code.co_name))
    except NameError:
        # options does not exist.
        return

def init():
    """collect option information, display help text if needed, set up debugging"""
    parser = OptionParser()
    env = {}
    parser.add_option("-u", "--url", type="string", dest="url",
                            default="http://cmdb.domain.com/nagios/",
                            help="Base Url.  Default='http://cmdb.domain.com/nagios/'")
    parser.add_option("-n", "--notification", action="store_true", dest="notification",
                            default=False,
                            help="treat as notification instead of alert event")
    parser.add_option("-r", "--rt", type="string", dest="rt",
                            help="location of rt script")
    parser.add_option("-j", "--jira", type="string", dest="jira",
                            default="/usr/local/bin/jira",
                            help="location of jira script")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                            default=False,
                            help="print debug messages to stderr")
    (options, args) = parser.parse_args()
    if options.verbose: sys.stderr.write(">>DEBUG sys.argv[0] running in " +
                            "debug mode\n")
    if options.rt and options.jira:
        print "Please pass only one of --rt, --jira."
        sys.exit(3)

    for item in os.environ.keys():
        if item.startswith('NAGIOS_'):
            short = item.replace('NAGIOS_', '')
            if short not in ['HOSTNAME', 'SERVICEDESC', 'HOSTSTATE', 'HOSTSTATETYPE',
                    'SERVICESTATE', 'SERVICESTATETYPE', 'HOSTOUTPUT', 
                    'SERVICEOUTPUT', 'LONGHOSTOUTPUT', 'LONGSERVICEOUTPUT']:
                continue
            if short == 'HOSTNAME':
                short = 'HOST'
            if short == 'SERVICEDESC' and os.environ[item]:
                short = 'SERVICE'
            short = short.lower()
            env[short] = os.environ[item].replace('\\n', '\n')
            if options.verbose:
                print "Adding %s('%s') to env dict." % (short, os.environ[item])
    if 'host' not in env:
        print "Missing Nagios environment variables.  Exiting."
        sys.exit(0)
    funcname(True, options.verbose)
    funcname(False, options.verbose)
    return options, env

def get_ticket(host, service=None):
    """Wait up to 60 seconds, attempting to get ticket."""
    ticket = None
    start = time.time()

    now = time.time()
    while now < start + 60:
        if service is None:
            subject = '- %s is' % (host)
        else:
            subject = '- %s/%s is' % (host, service)
        if options.rt:
            cmd = '%s ls -i "Subject like \'%%%s%%\' and Requestor like \'nagios%%\'' % \
                    (options.rt, subject)
            cmd += ' and ( Status = \'New\' or Status = \'Open\' ) and Created > \'%s\'"' % \
                    (datetime.date.today() - datetime.timedelta(days=7))
        else:
            cmd = '%s getissues "Summary~\'%%%s%%\' AND Resolution=Unresolved"' % \
                    (options.jira, subject.replace(' ', '%'))
        if options.verbose:
            print "About to run command: %s" % (cmd)
        ticket = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.readlines()
        if len(ticket) and ('ticket' in ticket[-1] or subject.replace('%', ' ') in ticket[-1]):
            # great, we have a ticket
            if options.rt:
                ticket = ticket[-1].split('/')[1]
            else:
                ticket = ticket[-1].split(',')[0]
            if options.verbose:
                print ticket
            break
        else:
            ticket = None
            time.sleep(5)
            now = time.time()

    return ticket

if __name__ == '__main__':
    options, env = init()
    try:
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, os.O_NONBLOCK)
        handler = sys.stdin.read()
    except:
        pass
    else:
        env['handler'] = handler
    if options.notification:
        url = options.url + 'addnotif/'
    else:
        url = options.url + 'addevent/'
    
    if options.notification:
        # this should be called via "echo 'blah | at now'"
        # otherwise nagios will block until this script finishes.
        if options.rt:
            ticketurl = 'https://rt.domain.com/Ticket/Display.html?id='
        elif options.jira:
            ticketurl = 'https://jira.domain.com/browse/'
        try:
            if env.has_key('service'):
                ticket = get_ticket(env['host'], env['service'])
            else:
                ticket = get_ticket(env['host'])
        except:
            # don't die on ticket failure
            ticket = None
            
        if ticket is not None:
            if env.has_key('service'):
                if not env.has_key('longserviceoutput'):
                    env['longserviceoutput'] = ''
                env['longserviceoutput'] += '\n%s%s' % (ticketurl, ticket)
            else:
                if not env.has_key('longhostoutput'):
                    env['longhostoutput'] = ''
                env['longhostoutput'] += '\n%s%s' % (ticketurl, ticket)
    if options.verbose:
        print "About to hit url %s" % (url)
    data = urllib.urlencode(env)
    if options.verbose:
        print ".. with data:"
        pprint.pprint(data)
    try:
        response = urllib2.urlopen(url, data)
    except urllib2.HTTPError, e:
        print e
        if options.verbose:
            print dir(e)
            print url, data
            for line in e.readlines():
                print line,
                print
    else:
        print response.read()
        if options.verbose:
            print

