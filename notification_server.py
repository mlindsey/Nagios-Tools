#!/usr/bin/python -u
# -*- coding: ascii -*-
"""Read nagios notification spool, parse configuration,
    handle notifications in a streamlined and enhanced way.
    Includes optional methods for enhanced notification.
    Stats config while running, allowing for restart-free
    config updates.  Fallback to last-known-good config
    if config is unparseable."""

import sys
import os
import time
import socket
import datetime
import signal
import threading
import fcntl
import pprint
import smtplib
import re
import shlex
import subprocess
import logging
import traceback
from simplejson import dumps, loads
from hashlib import md5
from platform import python_version
from copy import deepcopy
from inspect import getframeinfo
from thread import allocate_lock
from email.mime.text import MIMEText
from suds.client import Client

logging.getLogger('suds.client').setLevel(logging.CRITICAL)

try:
    import asdb
except:
    pass

major, minor = python_version().split('.')[0:2]
pyver = float('%s.%s' % (major, minor))
if pyver < 2.4:
    print "Requires Python 2.4 or higher."
    sys.exit(-1)

from optparse import OptionParser

# globals
log_levels = {}
log_error=log_levels['error']=4
log_warning=log_levels['warning']=3
log_info=log_levels['info']=2
log_debug=log_levels['debug']=1

depth=0

logfh = None
templates = {}
templateslock = allocate_lock()
spoolqueue_dict = {}
spoolqueue_dictlock = allocate_lock()
spoolqueue_dictitem = set()
spoolqueue_dictitemlock = {'master': allocate_lock()}
# thread-safe logging
logfilelock = allocate_lock()

notificationthread_dict = {}
stats_dict = {'emails': 0, 'emailfailures': 0, 'events': 0, 'tickets': 0, \
        'ticketappends': 0, 'ticketfailures': 0}
stats_dictlock = {'emails': allocate_lock(), 'emailfailures': allocate_lock(), \
        'events': allocate_lock(), 'tickets': allocate_lock(), \
        'ticketappends': allocate_lock(), 'ticketfailures': allocate_lock()}

def funcname(enter=True, log_level=None, console=False):
    """Display function name of parent function"""
    try:
        if (log_level and log_level == log_debug) or \
                (options.loglevel and options.loglevel == log_debug):
            global depth
            if enter:
                depth += 1
                log_mesg("DEBUG start - %s() [%i]" % \
                        (sys._getframe(1).f_code.co_name, depth), console=console)
            else:
                log_mesg("DEBUG end   - %s() [%i]" % \
                        (sys._getframe(1).f_code.co_name, depth), console=console)
                depth -= 1
    except NameError:
        # options does not exist.
        return

def init():
    """collect option information, display help text if needed, set up debugging"""
    default = {}
    help = {}

    usage = 'usage: %prog [options] [--start|--stop]\n'
    usage += 'startup:  %prog --start\n'
    usage += 'shutdown: %prog --stop\n'
    usage += 'status: %prog\n'
    usage += 'When run in status mode, exits with nagios style exit codes.\n'

    default['prefix'] = '/usr/local/nagios'
    default['loglevel'] = log_warning
    help['prefix'] = 'Nagios prefix.  Read config from $prefix/var/objects.cache\n'
    help['prefix'] += 'Spool into $prefix/notification_spool.\n'
    help['prefix'] += 'Log to $prefix/var/notification_server.log\n'
    help['prefix'] += 'Lockfile $prefix/var/notification_server.pid\n'
    help['prefix'] += 'Templates in $prefix/notification_templates/\n'
    help['prefix'] += 'Status file in $prefix/var/notifitication_server.stats\n'
    help['prefix'] += 'Default = %s' % (default['prefix'])
    help['loglevel'] = 'Process log level. 1-4\n'
    help['loglevel'] += '1=Full program flow, 2=Detailed info\n'
    help['loglevel'] += '3=Basic info, 4=Program startup, shutdown, and errors\n'

    parser = OptionParser(usage=usage)

    parser.add_option("-p", "--prefix", type="string", dest="prefix",
                            default=default['prefix'], help=help['prefix'])
    parser.add_option("-l", "--loglevel", type="int", dest="loglevel",
                            default=default['loglevel'],
                            help=help['loglevel'])
    parser.add_option("--timeout", type="int", dest="timeout",
                            default=20, help="Timeout for subscripts.")
    parser.add_option("--start", action="store_true", default=False,
                            help="Startup notification daemon.")
    parser.add_option("--stop", action="store_true", default=False,
                            help="Shutdown notification daemon.")
    parser.add_option("--restart", action="store_true", default=False,
                            help="Shut down, and then restart the notification daemon.")
    (options, args) = parser.parse_args()
    funcname(True, options.loglevel, console=True)
    error = False
    if not os.path.isdir(options.prefix):
        error = True
        print "Prefix '%s' is not a directory" % (options.prefix)
    else:
        if not os.path.isfile('%s/var/objects.cache' % (options.prefix)):
            error = True
            print "Object cache does not exist."
        if not os.path.isdir('%s/notification_templates/' % (options.prefix)):
            error = True
            print "Notification template directory missing."
        else:
            try:
                x = len(os.listdir('%s/notification_templates/' % (options.prefix)))
            except (OSError, IOError):
                error = True
                print "Notification template directory unreadable."
            else:
                if x == 0:
                    error = True
                    print "Template directory is empty."
        if not os.path.isdir('%s/notification_spool/' % (options.prefix)):
            error = True
            print "Notification spool directory is missing."
        else:
            try:
                x = len(os.listdir('%s/notification_spool/' % (options.prefix)))
            except (OSError, IOError):
                print "Notification spool directory unreadable."
        if not os.path.isdir('%s/var' % (options.prefix)):
            error = True
            print "%s/var/ does not exist." % (options.prefix)
        if not options.start and not options.stop:
            try:
                pidfh = open('%s/var/notification_server.pid' % (options.prefix), 'a')
            except (OSError, IOError):
                error = True
                print "PIDfile is not writable."
            else:
                pidfh.close()
            try:
                statfh = open('%s/var/notification_server.stats' % (options.prefix), 'a')
            except (OSError, IOError):
                error = True
                print "Status file is not writable."
            else:
                statfh.close()

    if options.restart:
        options.stop = True
        options.start = True
    if error:
        parser.print_help()
        sys.exit(2)
    funcname(False, options.loglevel, console=True)
    return options

def log_mesg(mesg, console=False):
    """Non-blocking synchronous writing logfile writer"""
    global logfh
    if logfh is None:
        logfh = os.open('%s/var/notification_server.log' % \
                (options.prefix), \
                        os.O_APPEND|os.O_SYNC|os.O_NONBLOCK|os.O_CREAT|os.O_WRONLY, \
                        0644)
        fcntl.fcntl(logfh, fcntl.F_SETFL, os.O_NONBLOCK)
    if console:
        print mesg
    before_lock = time.time()
    fcntl.flock(logfh, fcntl.LOCK_SH)
    logfilelock.acquire()
    after_lock = time.time()
    if pyver >= 2.5:
        os.lseek(logfh, 0, os.SEEK_END)
    else:
	#status = os.fstat(logfh)
        os.lseek(logfh, 0, 2)
    if (after_lock - before_lock) > 0.1:
        os.write(logfh, '[%s] (%s) %s (%ss)\n' % \
                (time.strftime("%Y/%m/%d %H:%M:%S"), str(os.getpid()), mesg, after_lock - before_lock))
    else:
        os.write(logfh, '[%s] (%s) %s\n' % \
                (time.strftime("%Y/%m/%d %H:%M:%S"), str(os.getpid()), mesg))
    logfilelock.release()
    os.fsync(logfh)
    fcntl.flock(logfh, fcntl.LOCK_UN)

def inspect_pid(console=False):
    funcname()
    ok = False
    try:
        pidfh = open('%s/var/notification_server.pid' % (options.prefix), 'r')
    except (OSError, IOError):
        if console:
            print "Missing pidfile."
    else:
        try:
            pid = int(pidfh.read())
        except (OSError, IOError, ValueError):
            if console:
                print "Invalid pid data."
        else:
            # Gotta clean this up with platform and path agnostic code

            cmd = "pgrep -f '%s'" % (os.path.basename(sys.argv[0]))
            ps = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.readlines()
            pslist = []
            for p in ps:
                pslist.append(int(p.strip()))
            if pid not in pslist:
                print "Notification server is not running."
            else:
                try:
                    os.kill(pid, 0)
                except (OSError, IOError):
                    if ps == '':
                        print "Notification server pid %i is not running." % (pid)
                    else:
                        if os.path.basename(sys.argv[0]) in ps:
                            print "Notification server pid %i is running," % (pid)
                            print "but with wrong owner!"
                else:
                    if console:
                        print "Notification server pid %i is running." % (pid)
                    ok = True
    funcname(False)
    return ok

def inspect_log(console=False):
    funcname()
    ok = False
    try:
        logfh = open('%s/var/notification_server.log' % (options.prefix), 'a')
    except (OSError, IOError):
        if console:
            print "Unwriteable logfile."
    else:
        logfh.close()
        ok = True
    funcname(False)
    return ok

def inspect_spool(console=False):
    funcname()
    ok = False
    try:
        spools = os.listdir('%s/notification_spool/' % (options.prefix))
    except (OSError, IOError):
        if console:
            print "Unable to read spool directory."
    else:
        if len(spools) > 100:
            if console:
                print "%i files in the spool directory!" % (len(spools))
        else:
            ok = True
    funcname(False)
    return ok

def inspect_status(console=False):
    funcname()
    ok = False
    try:
        statfh = open('%s/var/notification_server.stats' % (options.prefix), 'r')
    except (OSError, IOError):
        if console:
            print "Unreadable status file."
    else:
        statfh.close()
        status = os.stat('%s/var/notification_server.stats' % (options.prefix))
        if status.st_mtime < (time.time() - 240):
            if console:
                print "Status file is stale. (%i seconds old)" % (time.time() - status.st_mtime)
        else:
            ok = True
    funcname(False)
    return ok
            

def display_status():
    ok = inspect_pid(console=True) and \
            inspect_log(console=True) and \
            inspect_spool(console=True) and \
            inspect_status(console=True)
    if ok:
        sys.exit(0)
    else:
        sys.exit(2)

def cleanup():
    """Close down any active theads!"""
    funcname()
    log_mesg("Daemon exiting via SIGTERM")
    ImportTemplatesThread().shutdown()
    ReadSpoolThread().shutdown()
    # shutdown threads
    for thread in notificationthread_dict:
        if notificationthread_dict[thread].isAlive():
            notificationthread_dict[thread].shutdown()
    log_mesg("Threads exiting.")
    # wait for success
    for thread in notificationthread_dict:
        notificationthread_dict[thread].join()
    log_mesg("All threads exited cleanly")
    funcname(False)
    # just in case some thread got missed, and SystemExit is getting caught!
    os.kill(os.getpid(), 9)

def shutdown():
    """Shut the server down, waiting for thread exit."""
    funcname()
    pidfh = open('%s/var/notification_server.pid' % (options.prefix), 'r')
    pid = int(pidfh.read())
    down = False
    first = True
    slept = 0
    while not down:
        if first:
            print "Sending SIGTERM to pid %s, waiting for successful exit." % (pid),
        try:
            os.kill(pid, signal.SIGTERM)
        except (OSError, IOError):
            if not first:
                sys.stdout.write(".")
                try:
                    os.kill(pid, 0)
                except (OSError, IOError):
                    down = True
            else:
                first = False
        else:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                down = True
                print
            else:
                first = False

        time.sleep(1.5)
        slept += 1.5
        if slept > 15:
            print
            break
    if not down:
        print "PID %i not responding to SIGTERM." % (pid)
        try:
            os.kill(pid, 0)
        except (OSError, IOError):
            ps = subprocess.Popen("/bin/ps aux | /bin/grep %s | /bin/grep -v grep" % 
                    (pid).split()).read()
            if ps == '':
                print "Exited."
            else:
                print "Unable to kill daemon process %s, wrong owner, and you're not root!" % (pid)
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except (OSError, IOError):
                print "PID %i not responding to SIGKILL." % (pid)
            else:
                try:
                    os.kill(pid, 0)
                except (OSError, IOError):
                    print "Unclean exit."
                    log_mesg("Server killed uncleanly.")
                else:
                    print "Did not exit."


    else:
        cmd = "pgrep -f '%s'" % (os.path.basename(sys.argv[0]))
        ps = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.readlines()
        pslist = []
        for p in ps:
            pslist.append(int(p))
        if pid not in pslist:
            print "Exited."
        else:
            print "Unable to kill daemon process %s" % (pid)
    funcname(False)

def load_template(template):
    funcname()
    error = False
    template = template.split('\n')
    cleantemp = ''
    for line in template:
        if not line.startswith('#'):
            cleantemp += line
    template = cleantemp
    template = loads(template)
    template['last_refresh'] = int(time.time())
    for section in ['body', 'subject']:
        if section not in template:
            log_mesg('Missing %s in template' % (section))
            raise
    for section in template.keys():
        try:
            x = template[section].count(' ')
        except:
            continue
        else:
            if template[section].count('{% if ') != template[section].count('{% endif %}'):
                log_mesg('Inconsistent if/endif count in %s' % (section))
                error = True
        splits = template[section].split('{%')
        x = 1
        for split in splits[1::]:
            if '%}' not in split:
                log_mesg('Missing close tag near \'%s\'' % (split))
                error = True
            if split.count('%}') > 1:
                log_mesg('Missing open tag near \'%s\'' % (split))
                error = True
                    
                
            #if template[section].count('{%') != template[section].count('%}'):
            #    log_mesg('{%%:%s %%}:%s' % (template[section].count('{%'), template[section].count('%}')))
    funcname(False)
    return (template, error)

def import_templates(bad_templates={}):
    global templates
    if 'service_default' not in templates:
        templates['service_default'] = {
                'last_refresh': 0, # allows overriding by file
                'notification_type': 'email',
                'subject': '** {% env HOSTNAME %}/{% env SERVICEDESC %} is {% env SERVICESTATE %} **',
                'body': '{% env SERVICEOUTPUT %}\n{% env LONGSERVICEOUTPUT %}\n',
                'run_before': (),
                'run_after': ()}
    if 'host_default' not in templates:
        templates['host_default'] = {
                'last_refresh': 0, # allows overriding by file
                'notification_type': 'email',
                'subject': '** {% env HOSTNAME %} is {% env HOSTSTATE %} **',
                'body': '{% env HOSTOUTPUT %}',
                'run_before': (),
                'run_after': ()}

    loaded = 0
    for file in os.listdir('%s/notification_templates' % (options.prefix)):
        if file.startswith('.'):
            continue
        template = file
        file = '%s/notification_templates/%s' % (options.prefix, file)
        file_stat = os.stat(file)
        if template not in templates or \
                (template in templates and \
                        templates[template]['last_refresh'] < file_stat.st_mtime):
            if template in bad_templates and bad_templates:
                if file_stat.st_mtime < bad_templates[template]:
                    continue
            if options.loglevel <= log_info:
                log_mesg("Loading template '%s'" % (template))
            templatefh = open(file)
            try:
                (intemplate, error) = load_template(templatefh.read())
            except (OSError, IOError):
                log_mesg('Unable to load template from \'%s\'' % (template))
                bad_templates[template] = int(time.time())
            else:
                if error:
                    log_mesg('Unable to load template from \'%s\'' % (template))
                    bad_templates[template] = int(time.time())
                    continue
                templates[template] = intemplate
                if options.loglevel == log_debug:
                    log_mesg("Template dictionary: %s" % (templates[template]))
                loaded += 1
                if template in bad_templates:
                    del bad_templates[template]
            templatefh.close()
    if options.loglevel <= log_warning and loaded > 0:
        log_mesg("%s/%s templates loaded." % (loaded, len(templates)))
    return bad_templates

def read_spool(spoolfile):
    """Open, lock, read, and delete spool file, returning dictionary."""
    funcname()
    spool = None
    try:
        spoolfd = open(spoolfile, 'r+')
    except (OSError, IOError):
        log_mesg('Unable to open %s' % (spoolfile))
    else:
        try:
            fcntl.lockf(spoolfd, fcntl.LOCK_EX)
        except (OSError, IOError):
            log_mesg('Unable to lock %s' % (spoolfile))
        else:
            spool = spoolfd.read()
            try:
                spool = loads(spool)
            except ValueError:
                log_mesg('Unable to extract JSON from \'%s\', deleting \'%s\'' % \
                        (spool, os.path.basename(spoolfile)))
            else:
                if options.loglevel == log_debug:
                    log_mesg('Spool JSON: \'%s\'' % (spool))
            os.remove(spoolfile)
        spoolfd.close()
    funcname(False)
    return spool

class NotificationThread(threading.Thread):
    """
    One thread per notification event.
    A notification event is defined by a recently unique set if host & service & state
    """
    def __init__(self, spool_tuple):
        threading.Thread.__init__(self)
        self.spool_tuple = spool_tuple
        self._finished = threading.Event()
        self._duration = 120.0

    def setDuration(self, duration):
        self._duration = duration
    
    def shutdown(self):
        """Stop this thread"""
        self._finished.set()

    def run(self):
        log_mesg('Notification thread (%s) launched' % (self.getName()))
        global templates
        global templateslock
        global spoolqueue_dict
        global spoolqueue_dictlock
        global spoolqueue_dictitem
        global spoolqueue_dictitemlock
        global stats_dict
        global stats_dictlock
        stats_dictlock['events'].acquire()
        stats_dict['events'] += 1
        stats_dictlock['events'].release()
        idle = 0
        cache = {}
        start = time.time()

        sleep = 0.1
        first = 0
        # wait ten seconds for a ticket item
        ticketwait = 10 
        ticket = False
        items = []
        while time.time() < start + self._duration:
            if spoolqueue_dict.has_key(self.spool_tuple):
                acquire_lock(spoolqueue_dictlock, 'NT.spoolqueue_dictlock')
                acquire_lock(spoolqueue_dictitemlock[self.spool_tuple], \
                        'NT.spoolqueue_dictitemlock[%s]' % (str(self.spool_tuple)))
                items += spoolqueue_dict[self.spool_tuple]
                del spoolqueue_dict[self.spool_tuple]
                release_lock(spoolqueue_dictlock, 'NT.spoolqueue_dictlock')
                release_lock(spoolqueue_dictitemlock[self.spool_tuple], \
                        'NT.spoolqueue_dictitemlock[%s]' % (str(self.spool_tuple)))
            else:
                # increase sleep duration each time we don't have anything to process.
                if sleep < 5:
                    sleep = sleep * 1.5

                if options.loglevel == log_debug:
                    log_mesg('Notification thread got spoolqueue_dict for %s' % \
                            (str(self.spool_tuple)))
                if time.time() < start + ticketwait:
                    # still waiting for a potential ticket.
                    for item in items:
                        if templates[item['template']]['notification_type'] == 'jira' or \
                                templates[item['template']]['notification_type'] == 'host-jira' or \
                                (templates[item['template']].has_key('isticket') and 
                                        templates[item['template']]['isticket'] == True):
                            ticket = item
                # Either we've waited long enough, or we have a ticket.
                if ticket != False or time.time() > start + ticketwait:
                    if ticket != False:
                        # remove the ticket from the item list, make it first
                        try:
                            items.remove(ticket)
                            items.insert(0, ticket)
                        except:
                            # don't freak out if something odd happens and the
                            # ticket's been purged from the spool.
                            pass
                    for item in items:
                        this_time = time.time()
                        item, cache = expand_spoolitem(item, cache)
                        this_time = time.time() - this_time
                        if not item.has_key('nofooter'):
                            item['body'] += 'md5subject: %s\n' % (md5(item['subject']).hexdigest())
                            item['body'] += 'md5event: %s\n' % (md5(item['body']).hexdigest())
                            if first == 0:
                                item['body'] += '\nemail generated in %ss\n' % (this_time)
                                first = this_time
                            else:
                                item['body'] += '\nemail generated in %ss, saved %ss\n' % \
                                        (this_time, (first - this_time))
                            item['body'] += 'template: %s\n' % (item['template'])
                        if templates[item['template']]['notification_type'] == 'email':
                            try:
                                send_email(item['contactemail'], item['subject'], item['body'])
                            except:
                                log_mesg("Unhandled exception in send_email")
                        elif templates[item['template']]['notification_type'] == 'rt':
                            # Drop Sysops Alert tickets on the floor, handled through
                            # Procmail
                            if 'Sysops Alert' not in item['subject'] and \
                                    'ACKNOWLEDGEMENT' not in item['subject']:
                                try:
                                    send_ticket(item['contactemail'], item['subject'], item['body'])
                                except:
                                    log_mesg("Unhandled exception in send_ticket")
                                    send_email(item['contactemail'], item['subject'], item['body'])
                        elif templates[item['template']]['notification_type'] == 'jira':
                            # Drop Sysops Alert tickets on the floor, handled through
                            # Procmail
                            if 'Sysops Alert' not in item['subject'] and \
                                    'ACKNOWLEDGEMENT' not in item['subject']:
                                try:
                                    send_jira(item['contactemail'], item['project'], item['subject'], item['body'],
                                            {'product': item['product'], 'environment': item['environment'],
                                            'purpose': item['purpose']})
                                except:
                                    log_mesg("Unhandled exception in send_jira")
                                    if 'RECOVERY' not in item['subject']:
                                        item['body'] += '\n\nUnhandled exception in send_jira, '
                                        item['body'] += 'unable to set meta-data'
                                        send_email(item['contactemail'], item['subject'], item['body'])
                        elif templates[item['template']]['notification_type'] == 'nexus':
                            # Submit to Nexus
                            send_nexus(item)
                        else:
                            log_mesg('ERROR: item fell through handler check.')

                    items = []
            if self._finished.isSet():
                log_mesg('Notification thread exiting early but cleanly for %s' % \
                        (str(self.spool_tuple)))
                return
            time.sleep(sleep)
            idle += sleep
        if options.loglevel <= log_info:
            log_mesg('Notification thread exiting cleanly for %s' % (str(self.spool_tuple)))


def send_jira(to, project, subject, body, meta={}):
    """Create or update formatted JIRA ticket"""
    global stats_dict
    global stats_dictlock
    jira = '/usr/local/nagios/bin/jira'
    subject = subject.replace("'", '"')
    body = body.replace("'", '"')
    #body = re.sub(r'=*\nTicket Search(.*\n)*?md5', 'md5', body)

    log_mesg('Attempting to open ticket, to: %s, subject: \'%s\'' % (to, subject))
    if not os.path.isfile(jira):
        log_mesg('Attempting to open ticket, but jira script missing.  Falling back to email.')
        send_email(to, subject, body)
    else:
        cmd = '%s getissues "Summary ~ \'%s\' AND Reporter=nagios' % \
                (jira, subject.split(' alert - ')[1].split(' is ')[0].replace(' ', '%'))
        cmd += ' AND Resolution=Unresolved AND Project=%s AND Created > -14d"' % \
                (project)
        if options.loglevel <= log_warning:
            log_mesg('About to run \'%s\'' % (cmd))
        # strip the header, then force a sort
        try:
            ticket = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.readlines()[1::]
        except:
            ticket = []
        if len(ticket) and project in ticket[-1]:
            # great, we have a ticket to append to!
            ticket = ticket[-1].split(',')[0]
            cmd = '%s comment %s \'%s\'' % (jira, ticket, body.split('Date/Time', 1)[0])
            log_mesg('Appending data to existing ticket %s' % (ticket))
            log_mesg('About to run \'%s\'' % (cmd))
            out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT).stdout.read()
            if 'Comment added' not in out and 'RECOVERY' not in subject:
                # don't fall back to email if this is just a recovery!
                stats_dictlock['ticketfailures'].acquire()
                stats_dict['ticketfailures'] += 1
                stats_dictlock['ticketfailures'].release()
                body += 'script based ticket append failed, fallback to email\n'
                body += 'script cmd = \'%s\'' % (cmd)
                log_mesg('error out \'%s\'' % (out))
                send_email(to, subject, body)
            elif 'Comment added' in out:
                stats_dictlock['ticketappends'].acquire()
                stats_dict['ticketappends'] += 1
                stats_dictlock['ticketappends'].release()
                if 'RECOVERY' in subject and 'PAGING' not in body:
                    # close the ticket.
                    cmd = '%s cat %s' % (jira, ticket)
                    out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).stdout.read()
                    if 'Assignee: None' in out:
                        # first take the ticket.
                        cmd = '%s take %s' % (jira, ticket)
                        out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).stdout.read()
                        # Changes to queue logic has caused some queues to fail a take
                        cmd = '%s update %s assignee nagios' % (jira, ticket)
                        out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).stdout.read()
                        cmd = '%s resolve %s' % (jira, ticket)
                        out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).stdout.read()
                        if out != '':
                            log_mesg('Output from jira ticket resolve attempt: %s' % (out))
                        else:
                            log_mesg('Ticket closed.')
        elif 'RECOVERY' not in subject:
            # we have no ticket, and it's not a recovery.  create a new ticket
            hostname = subject.split(' - ')[1].split('/')[0].split()[0]
            # check for explicit priority, and override
            impacts = ['-1', '1 - Widespread', '2 - Significant', '3 - Moderate', '4 - Minor', '5 - No Impact']
            impact_id   = 'customfield_10224'
            urgencies = ['-1', '1 - Critical', '2 - High', '3 - Medium', '4 - Low']
            urgency_id  = 'customfield_10230'
            product_id  = 'customfield_10320'
            environment_id = 'customfield_10321'

            # Get Product
            product = None
            environment = None
            if 'no product from template' in meta['product'] or 'errEnv' in meta['product']:
                try:
                    product = asdb.cache('get_product_by_hostname', (hostname,), 86400)
                except:
                    pass
                else:
                    if product == 'fs':
                        try:
                            product = asdb.cache('get_product_by_service', (hostname, service,), 86400)
                        except:
                            pass
            else:
                product = meta['product']

            if product is None:
                product = 'UNKNOWN'

            # Get Environment
            if 'no environment from template' in meta['environment'] or 'errEnv' in meta['environment']:
                try:
                    environment = asdb.cache('get_environment_by_hostname', (hostname,), 86400)
                except:
                    pass
            else:
                environment = meta['environment']

            if environment is None:
                environment = 'ops'
            
            # parse for impact
            if '##i1##' in body:
                impact = impacts[1]
            elif '##i2##' in body:
                impact = impacts[2]
            elif '##i3##' in body:
                impact = impacts[3]
            elif '##i4##' in body:
                impact = impacts[4]
            else:
                impact = impacts[2]

            # parse for urgency
            if '##p1##' in body or '##u1##' in body:
                urgency = urgencies[1]
            elif '##p2##' in body or '##u2##' in body:
                urgency = urgencies[2]
            elif '##p3##' in body or '##u3##' in body:
                urgency = urgencies[3]
            elif '##p4##' in body or '##u4##' in body:
                urgency = urgencies[4]
            elif 'PAGING' in body:
                urgency = urgencies[2]
            else:
                urgency = urgencies[3]

            val = impacts.index(impact) + urgencies.index(urgency)
            if impact == 5:
                val += 1
            if val <= 3:
                priority = 1
            elif val == 4:
                priority = 2
            elif val == 5 or val == 6:
                priority = 3
            elif val == 7 or val == 8:
                priority = 4
            else:
                priority = 5

            duedate = datetime.date.today()
            if priority > 1:
                duedate = duedate + \
                        [datetime.timedelta(days=1), # P2
                         datetime.timedelta(days=7), # P3
                         datetime.timedelta(days=14)][(priority-2)] # P4
                                
            cmd = '%s create --project=%s --summary=\'%s\' --description=\'%s\' ' % \
                    (jira, project, subject, body)
            cmd += '--type=Incident --field=%s:"%s" --field=%s:"%s" --field=%s:"%s" --field=%s:"%s" ' % \
                    (impact_id, impact, urgency_id, urgency, product_id, product,
                    environment_id, environment)
            cmd += '-z P%i -D %sT18:00:00' % (priority, duedate.isoformat())
                    
            log_mesg('Creating new jira ticket for event')
            log_mesg('About to run \'%s\'' % (cmd))
            slept = 0
            out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT).stdout.readlines()[-1]
            log_mesg('Ticket creation result: %s' % (out))
            if 'Created' not in out:
                stats_dictlock['ticketfailures'].acquire()
                stats_dict['ticketfailures'] += 1
                stats_dictlock['ticketfailures'].release()
                log_mesg('error out \'%s\'' % (out))
                body += 'script based ticket create failed, fallback to email\n'
                body += 'script cmd = \'%s\'' % (cmd)
                send_email(to, subject, body)
            else:
                stats_dictlock['tickets'].acquire()
                stats_dict['tickets'] += 1
                stats_dictlock['tickets'].release()

def send_ticket(to, subject, body):
    """Create or update formatted RT ticket"""
    global stats_dict
    global stats_dictlock
    rt = '/usr/local/bin/rt'
    subject = subject.replace("'", '"')
    body = body.replace("'", '"')
    body = re.sub(r'=*\nTicket Search(.*\n)*?md5', 'md5', body)

    log_mesg('Attempting to open ticket, to: %s, subject: \'%s\'' % (to, subject))
    if not os.path.isfile(rt):
        log_mesg('Attempting to open ticket, but rt script missing.  Falling back to email.')
        send_email(to, subject, body)
    else:
        queue = to.split('@')[0]
        cmd = '%s ls -i "Subject like \'%s%%\' and Requestor like \'nagios%%\'' % \
                (rt, subject.split(' alert ')[1].split(' is ')[0])
        cmd += ' and ( Status = \'New\' or Status = \'Open\' ) and Created > \'%s\'"' % \
                (datetime.date.today() - datetime.timedelta(days=7))
        if options.loglevel <= log_warning:
            log_mesg('About to run \'%s\'' % (cmd))
        ticket = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.readlines()
        if len(ticket) and 'ticket' in ticket[-1]:
            # great, we have a ticket to append to!
            ticket = ticket[-1].split('/')[1]
            cmd = '%s correspond -m \'%s\' %s' % (rt, body, ticket)
            log_mesg('Appending data to existing ticket %s' % (ticket))
            out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.read()
            if 'recorded' not in out and 'RECOVERY' not in subject:
                # don't fall back to email if this is just a recovery!
                stats_dictlock['ticketfailures'].acquire()
                stats_dict['ticketfailures'] += 1
                stats_dictlock['ticketfailures'].release()
                body += 'script based ticket append failed, fallback to email\n'
                body += 'script cmd = \'%s\'' % (cmd)
                send_email(to, subject, body)
            elif 'recorded' in out:
                stats_dictlock['ticketappends'].acquire()
                stats_dict['ticketappends'] += 1
                stats_dictlock['ticketappends'].release()
        elif 'RECOVERY' not in subject:
            # we have no ticket, and it's not a recovery.  create a new one ticket
            hostname = subject.split(' - ')[1].split('/')[0]
            # check for explicit priority, and override
            if '##p1##' in body:
                priority = 1
                duedelta = datetime.timedelta(hours=6)
            elif '##p2##' in body:
                priority = 2
                duedelta = datetime.timedelta(days=1)
            elif '##p3##' in body:
                priority = 3
                duedelta = datetime.timedelta(days=7)
            elif '##p4##' in body:
                priority = 4
                duedelta = datetime.timedelta(days=14)
            elif 'PAGING' in body:
                priority = 2
                duedelta = datetime.timedelta(days=1)
            else:
                priority = 3
                duedelta = datetime.timedelta(days=7)
            cmd = '%s new -t ticket set subject=\'%s\' queue=\'%s\' Due=\'%s\' text=\'%s\' ' % \
                        (rt, subject, to.split('@')[0], (datetime.datetime.today() + duedelta), body)
            cmd += 'Priority=%s CF-SysopsType=\'alert\'' % (priority)
            product = 'ops'
            try:
                product = asdb.get_product_by_hostname(hostname)
            except:
                pass
            else:
                if product == 'fs':
                    try:
                        product = asdb.get_product_by_service(hostname, service)
                    except:
                        pass
            cmd += ' CF-SysopsProduct=\'%s\'' % (product)
            try:
                environment = asdb.get_environment_by_hostname(hostname)
            except:
                pass
            else:
                cmd += ' CF-SysopsEnvironment=\'%s\'' % (environment)
            log_mesg('Creating new ticket for event')
            slept = 0
            out = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE).stdout.read()
            if 'created' not in out:
                stats_dictlock['ticketfailures'].acquire()
                stats_dict['ticketfailures'] += 1
                stats_dictlock['ticketfailures'].release()
                body += 'script based ticket append failed, fallback to email\n'
                body += 'script cmd = \'%s\'' % (cmd)
                send_email(to, subject, body)
            else:
                log_mesg('Ticket creation result: %s' % (out))
                stats_dictlock['tickets'].acquire()
                stats_dict['tickets'] += 1
                stats_dictlock['tickets'].release()

def send_email(to, subject, body):
    """Send a formatted email to a contact."""
    global stats_dict
    global stats_dictlock
    mesg = MIMEText(body)
    mesg['To'] = to
    mesg['Subject'] = subject
    # Make this configurable.
    mesg['From'] = 'Nagios Pseudo-user <nagios@nagios.domain.com>'
    try:
        s = smtplib.SMTP('localhost', 25)
    except (socket.error, socket.gaierror):
        log_mesg('Unable to connect to local mail server for \'%s\' \'%s\'' % (to, subject))
        stats_dictlock['emailfailures'].acquire()
        stats_dict['emailfailures'] += 1
        stats_dictlock['emailfailures'].release()
    else:
        log_mesg('Sending email \'%s\' \'%s\'' % (to, subject))
        # no try/except here, as, as far as I can tell, this never throws
        # an exception!
        s.sendmail(mesg['From'], [to], mesg.as_string())
        stats_dictlock['emails'].acquire()
        stats_dict['emails'] += 1
        stats_dictlock['emails'].release()
        s.quit()

def parse_env(command, item, i):
    """Parse out item from environment dict"""
    e = False
    command = command.lower()
    if command in item:
        i += item[command]
    else:
        e = True
        i += 'errEnv\'%s\'' % (command)
    return i, e

def variable_sub(command, item, quote=True):
    """Allow $VAR$ substitution"""
    for x in xrange(len(command)):
        substr = command[x]
        if (substr[0] == '$' and substr[-1] == '$') or \
                (((substr[0] == '\'' or substr[-1] == '"') and \
                (substr[0] == '\'' or substr[-1] == '"')) and \
                (substr[1] == '$' and substr[-2] == '$')):
            substr = substr.strip('$"\'').lower()
            if substr in item:
                command[x] = item[substr]
                command[x] = command[x].replace('\\', '\\\\')
                command[x] = command[x].replace('\'', '\\\'')
                command[x] = command[x].replace('"', '\\"')
                command[x] = command[x].replace('(', '\\(')
                if len(command[x].split()) > 1 and quote:
                    command[x] = '"%s"' % (command[x])
    return command

def parse_script(command, item, i, cache):
    if 'script' in command:
        log_mesg('script command \'%s\', aborting' % (command))
        return i, cache
    if command in cache:
        i += cache[command]
    else:
        if options.loglevel <= log_warning:
            log_mesg('script command \'%s\'' % (command))
        try:
            out = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except:
            cache[command] = 'error launching command %s\n' % (command)
            i += cache[command]
            return i, cache
        (r, e) = (out.stdout, out.stderr)
        fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(e, fcntl.F_SETFL, os.O_NONBLOCK)
        slept = 0
        output = ''
        w.close()
        while slept <= options.timeout:
            try:
                output += r.read()
            except IOError:
                slept += 0.1
                time.sleep(0.1)
                continue
            else:
                break
        else:
            output += '(script timeout after %ss)' % (options.timeout)
            log_mesg('Timed out running \'%s\', after %ss.' % \
                    (command, options.timeout))
        try:
            output += e.read()
        except IOError:
            pass
        r.close()
        e.close()
        cache[command] = output
        i += cache[command]
    return i, cache

def parse_if(command, section, sectiontext, i, item, cache):
    """Parse if/if not/else/if"""
    invert = False
    stringtest = False
    instring = ''
    if command[0] == 'not':
        invert = True
        command.remove('not')
    if command[0][0] == '\'' and command[0][-1] == '\'':
        stringtest = True
        instring = command.pop(0).strip('\'')
        command.pop(0) # remove the 'in'
    if command[0] == 'script':
        command = variable_sub(command[1::], item)
        cmd = ' '.join(command)
        # we don't care about the return of item, but we do
        # care about the cache
        i, cache = parse_script(cmd, item, '', cache)
        if options.loglevel == log_debug:
            log_mesg('if string, stringtest:%s, instring:%s, cache[command]:%s, invert:%s' % \
                    (stringtest, instring, cache[cmd], invert))
        while command[0] != 'endif':
            if (stringtest and instring in cache[cmd] and not invert) or \
                    (stringtest and instring not in cache[cmd] and invert) or \
                    (not stringtest and cache[cmd] and not invert) or \
                    (not stringtest and not cache[cmd] and invert):
                i, cache, sectiontext, command, done = \
                        expand_loopcore(i, item, cache, section, sectiontext)
                if command[0] == 'else':
                    command[0] = 'endif'
                    sectiontext = sectiontext.split('{% endif %}', 1)[1]
            else:
                sectiontest = sectiontext.split('{% endif %}', 1)[0]
                if sectiontest.count('{% else %}'):
                    invert = not invert
                    sectiontext = sectiontext.split('{% else %}', 1)[1]
                else:
                    command[0] = 'endif'
                    sectiontext = sectiontext.split('{% endif %}', 1)[1]
    if command[0] == 'env':
        cmd = command[1]
        cmd = cmd.lower()
        if options.loglevel == log_debug:
            log_mesg('command:\n%s\nitem:\n%s\n' % (pprint.pformat(cmd), pprint.pformat(item)))
        while command[0] != 'endif':
            if (item.has_key(cmd) and ((stringtest and instring in item[cmd] and not invert) or \
                    (stringtest and instring not in item[cmd] and invert) or \
                    (not stringtest and item[cmd] and not invert) or \
                    (not stringtest and not item[cmd] and invert))):
                i, cache, sectiontext, command, done = \
                        expand_loopcore(i, item, cache, section, sectiontext)
                if command[0] == 'else':
                    command[0] = 'endif'
                    sectiontext = sectiontext.split('{% endif %}', 1)[1]
            else:
                sectiontest = sectiontext.split('{% endif %}', 1)[0]
                if sectiontest.count('{% else %}'):
                    invert = not invert
                    sectiontext = sectiontext.split('{% else %}', 1)[1]

                else:
                    command[0] = 'endif'
                    sectiontext = sectiontext.split('{% endif %}', 1)[1]
    return sectiontext, i, item, cache

def parse_func(command, item, i):
    """Parse limited functions"""
    e = False
    def strip_and_camel(*args):
        """Strip a string down for wiki. """
        text = args[0]
        try:
            fmt = args[1]
        except IndexError:
            fmt = None
        if fmt and fmt == 'service':
            text = text.capitalize()
            text = text.split('__')[0] # trim down to basics
            text = re.sub('[/._\- ]', '', text) # strip invalid chars
        else:
            text = text.split('.')[0].capitalize()
            text = re.sub('[0-9_-]', '', text)
        return text
    allowed_funcs = {'strip_and_camel': strip_and_camel}
    if command[0] in allowed_funcs:
        command = variable_sub(command, item, quote=False)
        args = command[1::]        
        text = allowed_funcs[command[0]](*args)
        i += text
    else:
        e = True
        i += 'errFunc \'%s\'' % (command[0])
    return item

def expand_spoolitem(item, cache):
    """Inspect a spool item, expand it based on template."""
    funcname()
    global templateslock
    acquire_lock(templateslock, 'templateslock')
    template = deepcopy(templates[item['template']])
    release_lock(templateslock, 'templateslock')
    for section in template.keys():
        done = False
        sectiontext = str(template[section])
        i = ''
        while not done:
            # clean this up, ugly!
            if (options.loglevel == log_debug):
                log_mesg('section:\n%s\nitem:\n%s\nsectiontext:\n%s' %
                        (section, item[section], sectiontext)) 
            i, cache, sectiontext, command, done = \
                    expand_loopcore(i, item, cache, section, sectiontext)
            if (options.loglevel == log_debug):
                log_mesg('section:\n%s\nitem:\n%s\nsectiontext:\n%s' %
                        (section, item[section], sectiontext)) 
        item[section] = i
    for section in ['subject', 'body', 'project', 'product', 'environment', 'purpose']:
        if not template.has_key(section):
            item[section] = '(no %s from template)' % (section)
            if section == 'body':
                item[section] += '\n%s' % (pprint.pformat(item))
    funcname(False)
    return item, cache

def expand_loopcore(i, item, cache, section, sectiontext):
    """core of loop, so we can call recursively from if"""
    done = False
    command = []
    subsplit = sectiontext.split('{%', 1)
    i += subsplit[0]
    if len(subsplit) == 1:
        done = True
    else:
        command = subsplit[1].split('%}', 1)
        sectiontext = command[1]
        command = command[0].split()
        for x in xrange(len(command)):
            command[x].strip()
        if command[0] == 'env':
            i, e = parse_env(command[1], item, i)
        elif command[0] == 'func':
            i = parse_func(command[1::], item, i)
        elif command[0] == 'script':
            command = variable_sub(command[1::], item)
            command = ' '.join(command)
            i, cache = parse_script(command, item, i, cache)
        elif command[0] == 'if':
            sectiontext, i, item, cache = parse_if(command[1::], section, sectiontext, i, item, cache)
    return i, cache, sectiontext, command, done

class ImportTemplatesThread(threading.Thread):
    """
    Perisistant thread for reading templates.
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self._finished = threading.Event()

    def shutdown(self):
        """Stop this thread"""
        self._finished.set()

    def run(self):
        log_mesg('Template importing thread initializing.')
        global templates
        global templateslock
        while 1:
            if self._finished.isSet():
                return
            slept = 0
            acquire_lock(templateslock, 'ITT.templateslock')
            import_templates()
            release_lock(templateslock, 'ITT.templateslock')
            while slept < 10:
                if self._finished.isSet():
                    log_mesg('Template imporing thread exiting cleanly.')
                    return
                time.sleep(0.1)
                slept += 0.1

class ReadSpoolThread(threading.Thread):
    """
    Perisistant thread for reading spool.
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self._finished = threading.Event()

    def shutdown(self):
        """Stop this thread"""
        self._finished.set()

    def run(self):
        log_mesg('Spool reader thread initializing.')
        global templates
        global templateslock
        global spoolqueue_dict
        global spoolqueue_dictlock
        spool_dir = '%s/notification_spool/' % (options.prefix)
        while 1:
            spools = os.listdir(spool_dir)
            for spoolfile in spools:
                if spoolfile.startswith('.'):
                    continue
                log_mesg('Processing %s from spool' % (spoolfile))
                spool = read_spool('%s%s' % (spool_dir, spoolfile))
                try:
                    x = spool.has_key('hostname')
                except:
                    log_mesg('Error reading spoolfile %s\nContents:%s' % (spoolfile, spool))
                    continue
                if ((spool.has_key('hostname') or spool.has_key('hostaddress')) 
                        and spool.has_key('contactemail')):
                    if spool.has_key('servicedesc'):
                        # service
                        service = spool['servicedesc']
                        state = spool['servicestate']
                    else:
                        service = None
                        state = spool['hoststate']
                    try:
                        hostname = spool['hostname']
                    except:
                        hostname = spool['hostaddress']
                    contact = spool['contactemail']
                    if spool.has_key('template') and spool['template'] in templates:
                        template = spool['template']
                    else:
                        if service is None:
                            spool['template'] = 'service_default'
                            template = 'service_default'
                        else:
                            spool['template'] = 'host_default'
                            template = 'host_default'

                    if service is None:
                        log_mesg('Processing host notification to \'%s\' about %s/%s' % \
                                (contact, hostname, state))
                        spool_tuple = (hostname, '', state)

                    else:
                        log_mesg('Pushing service notification to \'%s\' about %s/%s with \'%s\' onto the stack' % \
                                (contact, hostname, service, template))
                        spool_tuple = (hostname, service, state)
                    # Lock queue dictionary
                    acquire_lock(spoolqueue_dictlock, 'RST.spoolqueue_dictlock')
                    if spool_tuple not in spoolqueue_dictitem:
                        # no item lock available, allocate
                        acquire_lock(spoolqueue_dictitemlock['master'], \
                                'RST.spoolqueue_dictitemlock[\'master\']')
                        spoolqueue_dictitemlock[spool_tuple] = allocate_lock()
                        spoolqueue_dictitem.add(spool_tuple)
                        release_lock(spoolqueue_dictitemlock['master'], \
                                'RST.spoolqueue_dictitemlock[\'master\']')
                    # acquire item lock
                    acquire_lock(spoolqueue_dictitemlock[spool_tuple], \
                            'RST.spoolqueue_dictitemlock[\'%s\']' % (str(spool_tuple)))
                    if spool_tuple in spoolqueue_dict:
                        # append item to spool queue
                        spoolqueue_dict[spool_tuple].append(spool)
                    else:
                        # create single-item spool queue
                        spoolqueue_dict[spool_tuple] = [spool]
                    release_lock(spoolqueue_dictlock, 'RST.spoolqueue_dictlock')
                    release_lock(spoolqueue_dictitemlock[spool_tuple], \
                            'RST.spoolqueue_dictitemlock[\'%s\']' % (str(spool_tuple)))
                else:
                    log_mesg('Nothing to process for spool \'%s\'' % (str(spool)))
            slept = 0
            while slept < 5:
                if self._finished.isSet():
                    log_mesg('Spool reading thread exiting cleanly.')
                    return
                time.sleep(0.1)
                slept += 0.1

def acquire_lock(lock, name=''):
    if options.loglevel <= log_info:
        (filename, lineno, function, code_context, index) = getframeinfo(sys._getframe(1))
        log_mesg('About to acquire lock \'%s\' for %s.%s' % \
                (name, function, lineno))
    lock.acquire()
    if options.loglevel <= log_info:
        log_mesg('Acquired lock \'%s\' for %s.%s' % \
                (name, function, lineno))

def release_lock(lock, name=''):
    if options.loglevel <= log_info:
        (filename, lineno, function, code_context, index) = getframeinfo(sys._getframe(1))
        log_mesg('About to release lock \'%s\' for %s.%s' % \
                (name, function, lineno))
    lock.release()
    if options.loglevel <= log_info:
        log_mesg('Released lock \'%s\' for %s.%s' % \
                (name, function, lineno))

def run_daemon():
    funcname()
    signal.signal(signal.SIGTERM, lambda signum, stack_frame: cleanup())
    # close stdin, stdout, stderr
    #os.close(0)
    #os.close(1)
    #os.close(2)
    spool_dir = '%s/notification_spool/' % (options.prefix)
    # Preload templates, incase data is in the spool on startup.
    import_templates()
    _ITT = ImportTemplatesThread()
    _RST = ReadSpoolThread()
    _ITT.start()
    w.write("Import Templates Thread started.\n")
    w.flush()
    _RST.start()
    w.write("Read Spool Thread started.\n")
    w.flush()
    w.close()
    global spoolqueue_dict
    global spoolqueue_dictlock
    global spoolqueue_dictitem
    global spoolqueue_dictitemlock
    global notificationthread_dict
    global stats_dict
    global stats_dictlock
    x = 0
    minute_counter = time.time()
    # loop forever
    while 1:
        # restart Template and Spoolthreads if dead
        if not _ITT.isAlive():
            log_mesg('ImportTemplatesThread died!')
            _ITT.start()
        if not _RST.isAlive():
            log_mesg('ReadSpoolThread died!')
            _RST.start()
        # Get the lock, grab the keys, then release so the reader can
        # do it's job
        acquire_lock(spoolqueue_dictlock, 'spoolqueue_dictlock')
        spool_tuples = spoolqueue_dict.keys()
        release_lock(spoolqueue_dictlock, 'spoolqueue_dictlock')
        for spool_tuple in spool_tuples:
            if spool_tuple in notificationthread_dict and \
                    not notificationthread_dict[spool_tuple].isAlive():
                del notificationthread_dict[spool_tuple]
            sys.stdout.flush()
            acquire_lock(spoolqueue_dictitemlock[spool_tuple], \
                    'spoolqueue_dictitemlock[%s]' % (str(spool_tuple)))
            if spool_tuple not in notificationthread_dict:
                notificationthread_dict[spool_tuple] = NotificationThread(spool_tuple)
                notificationthread_dict[spool_tuple].setName(str(spool_tuple))
            if spool_tuple in spoolqueue_dict and len(spoolqueue_dict[spool_tuple]) and \
                    not notificationthread_dict[spool_tuple].isAlive():
                release_lock(spoolqueue_dictitemlock[spool_tuple], \
                        'spoolqueue_dictitemlock[%s]' % (str(spool_tuple)))
                try:
                    notificationthread_dict[spool_tuple].start()
                except:
                    log_mesg('Error (re)starting for %s' % (str(spool_tuple)))
            else:
                release_lock(spoolqueue_dictitemlock[spool_tuple], \
                        'spoolqueue_dictitemlock[%s]' % (str(spool_tuple)))
        if time.time() > minute_counter + 60:
            stats = ''
            for item in stats_dict:
                stats += '%s:%s ' % (item, stats_dict[item])
            stats.strip()
            stats += '\n'
            statfh = open('%s/var/notification_server.stats' % (options.prefix), 'w')
            statfh.write(stats)
            statfh.close()
            minute_counter = time.time()
        # replace this with code letting spool reader wake up 
        # this thread?
        time.sleep(0.5)
    funcname(False)

if __name__ == '__main__':
    options = init()
    if not options.start and not options.stop:
        display_status()
    if options.stop:
        if inspect_pid() is False:
            pass
        else:
            shutdown()
    if options.restart:
        time.sleep(1)
    if options.start:
        if inspect_pid():
            pidfh = open('%s/var/notification_server.pid' % (options.prefix), 'r')
            pid = pidfh.read()
            pidfh.close()
            print "Notification Server is already running as pid %s." % (pid)
            sys.exit()
        r, w = os.pipe()
        pid = os.fork()
        if pid:
            print "Forking child into background daemon as pid %i." % (pid)
            os.close(w)
            fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK) 
            r = os.fdopen(r)
            while 1:
                try:
                    status = r.readline()
                except IOError:
                    time.sleep(0.01)
                    continue
                if not status:
                    break
                else:
                    print status,
        else:
            os.close(r)
            fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK) 
            w = os.fdopen(w, 'w')
            mypid = os.getpid()
            try:
                pidfh = open('%s/var/notification_server.pid' % (options.prefix), 'w')
            except IOError:
                w.write("Child unable to write pidfile!\n")
                w.flush()
                w.close()
                sys.exit(2)
            else:
                w.write("Child forked successfully.\n")
                log_mesg("Server started successfully.")
                pidfh.write(str(mypid))
                pidfh.close()
                # Fork again before launching daemon, and leave a non-threaded supervisor
                # process?
                run_daemon()
                log_mesg("Server exited abnormally!")
            

