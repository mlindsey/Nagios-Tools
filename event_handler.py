#!/usr/bin/python

import os
import sys
from datetime import datetime
from subprocess import Popen, PIPE
from shlex import split

try:
    envs = sys.argv[1]
except:
    print "pass 'name:value|name2:value' pairs"
    print "ie: 'hostname:testhost|hostaddress:1.2.3.4|hoststate:DOWN|hoststatetype:SOFT|hostdowntime:0|hostattempt:2|maxhostattempts:3|hostoutput:output'"
    sys.exit()

e = {}
base    = '/usr/local/nagios/event_handler'
binpath = '/usr/local/nagios/bin'
url     = 'https://nagios.domain.com/nagios'

for env in envs.split('|'):
    (n, v) = env.split(':',1)
    e[n.lower()] = v

try:
    name = e['hostname']
    if not e.has_key('servicestate'):
        # Host issue
        full  = name
        descr = e['hostaddress']
        state = e['hoststate']
        stype = e['hoststatetype']
        attempt = e['hostattempt']
        attmax  = e['maxhostattempts']
        downt = e['hostdowntime']
        output = e['hostoutput']
    else:
        # Service issue
        descr = e['servicedesc']
        full  = '%s/%s' % (name, descr)
        state = e['servicestate']
        stype = e['servicestatetype']
        attempt = e['serviceattempt']
        attmax  = e['maxserviceattempts']
        downt = e['servicedowntime']
        output = e['serviceoutput']
except KeyError:
    print 'please pass at minimum, hostname, address, state, state type, attempt,'
    print 'max attempts, downtime, and output'
    sys.exit(0)

if (state == 'CRITICAL') or (state == 'DOWN') and downt == '0' and stype == 'SOFT':
    # determine if pageable
    if not e.has_key('servicestate'):
        cmd = '%s/nagios_escalation.py -H %s' % (binpath, descr)
        _url = '%s/cgi-bin/extui.py?host=%s'
    else:
        cmd = '%s/nagios_escalation.py -H %s -S "%s"' % (binpath, e['hostaddress'], descr)
        _url = '%s/cgi-bin/extui.py?host=%s&service=%s'
    c = Popen(split(cmd), stdout=PIPE)
    (out, err) = c.communicate()
    if 'pager' in out:
        if attempt == '1':
            # Wall this host for early warning.
            try:
                c = Popen('/usr/bin/wall', stdin=PIPE)
                c.communicate(input='%s [%s/%s] \'%s\' %s' % (full, attempt, attmax, output, url))
            except:
                print 'Unable to complete early-warning wall.'
        elif attempt > (attmax / 2.0):
            # Notify IRC if almost paging.
            try:
                c = Popen(split('%s/ironcat.sh "%s [%s/%s] \'%s\' %s"' % (full, attempt, attmax, output, url)))
            except:
                print 'Unable to notify IRC.'

        
if downt == '0':
    if stype == 'HARD':
        script = '/usr/local/nagios/event_handler/%s/HARD/' % (state)
    else:
        script = '/usr/local/nagios/event_handler/%s/SOFT/%s/' % (state, attempt)

    if not e.has_key('servicestate'):
        script += name
    else:
        _script = script + name + '_' + descr.lower().replace(' ', '_').replace('/', '')
        if os.path.isfile(_script): # and os.access(_script, os.X_OK):
            script == _script
        else:
            script += descr.lower().replace(' ', '_').replace('/', '')
        
    date = datetime.now().ctime()
    for key in e.keys():
        os.environ['NAGIOS_%s' % (key.upper())] = e[key]
    if os.path.isfile(script) and os.access(script, os.X_OK):
        # script exists.  Generate environment variables and then call
        c = Popen(script, stdout=PIPE, stderr=PIPE)
        (out, err) = c.communicate()
        # Clean this up.  Perhaps use syslog calls.
        f = open('/tmp/handler.out', 'a')
        f.write('[%s] %s %s\n' % (date, script, name))
        f.write('%s\n' % (out))
        f.write('%s\n\n' % (err))
        f.close()

        try:
            c = Popen('%s/nagsub.py' % (binpath), stdin=PIPE, stdout=PIPE, stderr=PIPE)
            (out, err) = c.communicate(input='%s\n%s\n%s' % (script, out, err))
        except:
            print 'Unable to submit to event tracking database.'
    else:
        # Clean this up.  Perhaps use syslog calls.
        f = open('/tmp/handler.out', 'a')
        f.write('[%s] (no) %s %s\n' % (date, script, name))
        f.close()
        try:
            c = Popen('%s/nagsub.py' % (binpath), stdout=PIPE, stderr=PIPE)
        except:
            print 'Unable to submit to event tracking database.'
        

