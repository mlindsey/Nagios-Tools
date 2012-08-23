#!/usr/bin/python -u
# -*- coding: ascii -*-
"""Collect nagios environment variables, dump them into a spool
    directory, triggering enhanced notifications."""

import sys
import os
import shutil
import simplejson
import tempfile

from optparse import OptionParser

def funcname(enter=True, forceverbose=False):
    """Display function name of parent function"""
    try:
        if forceverbose or options.verbose:
            if enter:
                sys.stderr.write(">>DEBUG start - %s()\n" % (sys._getframe(1).f_code.co_name))
            else:
                sys.stderr.write(">>DEBUG end   - %s()\n" % (sys._getframe(1).f_code.co_name))
    except NameError:
        # options does not exist.
        return

def init():
    """collect option information, display help text if needed, set up debugging"""
    parser = OptionParser()
    default = {}
    help = {}
    default['prefix'] = '/usr/local/nagios'
    help['prefix'] = 'Nagios prefix.  Read config from $prefix/etc.\n'
    help['prefix'] += 'Spool into $prefix/notification_spool.\n'
    help['prefix'] += 'Default = %s' % (default['prefix'])
    parser.add_option("-p", "--prefix", type="string", dest="prefix",
                            default=default['prefix'], help=help['prefix'])
    parser.add_option("-t", "--template", type="string", default=None,
                            help="Output template to use.")
    parser.add_option("-H", "--hostname", type="string", dest="hostname",
                            help="Override Hostname")
    parser.add_option("-S", "--service", type="string", dest="servicedesc",
                            help="Override ServiceDesc")
    parser.add_option("-c", "--contactemail", type="string", dest="contactemail",
                            help="Override Contact Email address.")
    parser.add_option("-m", "--macros", type="string", dest="macros",
                            help="Single-Quoted Pipe-Delimited Name:Value pairs.",
                            metavar="'<MACRO>:<VALUE>|...'")
    parser.add_option("-n", "--noenv", action="store_true", dest="noenv",
                            help="Don't read the environment, only use explicitly passed macros.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                            default=False,
                            help="print debug messages to stderr")
    (options, args) = parser.parse_args()
    if options.verbose: sys.stderr.write(">>DEBUG sys.argv[0] running in " +
                            "debug mode\n")
    funcname(True, options.verbose)
    if not options.prefix:
        sys.stderr.write("Missing --prefix\n")
	parser.print_help()
        sys.exit(3)
    funcname(False, options.verbose)
    return options

def add_to_spool():
    """Reads through environment variables and dumps NAGIOS_* to a spool
    file as a JSON dictionary"""
    funcname()
    try:
        (spool_fd, spool_fname) = tempfile.mkstemp(prefix='n')
    except:
        if options.verbose:
            sys.stderr.write("Unable to make temp_file '%s'\n" % (spool_fname))
    else:
        try:
            spool_file = os.fdopen(spool_fd, 'w')
        except:
            if options.verbose:
                sys.stderr.write("Unable to open spool_fd\n")
        else:
            environment = {}
            if not options.noenv:
                for env in os.environ.keys():
                    if env.startswith('NAGIOS_'):
                        env_strip = env.replace('NAGIOS_','').lower()
                        # unescape newlines before feeding to the spool!
                        environment[env_strip] = os.environ[env].replace('\\n', '\n')
                        # override the contactemail if needed.
                        if env_strip == 'contactemail' and options.contactemail:
                            environment[env_strip] = options.contactemail
            # Explicitly passed macros take precedence over environment.
            if options.macros is not None:
                for macro in options.macros.split('|'):
                    (name, value) = macro.split(':', 1)
                    environment[name] = value
                    
            environment['template'] = options.template
            if options.hostname is not None:
                environment['hostname'] = options.hostname
            if options.servicedesc is not None:
                environment['servicedesc'] = options.servicedesc
            if options.contactemail is not None:
                environment['contactemail'] = options.contactemail
            environment = simplejson.dumps(environment)
            try:
                spool_file.write(environment)
            except:
                if options.verbose:
                    sys.stderr.write("Unable to write out JSON to '%s'\n" % (spool_fname))
                    sys.stderr.write(environment)
            else:
                spool_file.close()
                fname = '%s' % (spool_fname.split('/')[-1])
                dest = '%s/notification_spool' % (options.prefix)
                if not os.path.exists(dest):
                    try:
                        os.mkdir(dest)
                    except:
                        if options.verbose:
                            sys.stderr.write("Missing spooldir '%s' and unable to create\n" % (dest))
                try:
                    shutil.move(spool_fname, '%s/%s' % (dest, fname))
                except:
                    if options.verbose:
                        sys.stderr.write("Unable to copy spool file '%s' to dest '%s'\n" % (spool_fname, dest))
    funcname(False)

if __name__ == '__main__':
    options = init()
    add_to_spool()
