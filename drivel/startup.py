from __future__ import with_statement
from glob import glob
from optparse import OptionParser
import os
import signal
import socket
import sys

import daemon  # provided by python-daemon

from drivel.config import fromfile as config_fromfile
from drivel.server import start


def lifecycle_cleanup():
    """Terminate the process nicely."""
    sys.exit(0)


def lifecycle_start(conf, options):
    with daemon.DaemonContext() as dctx:
        # Write the pid
        with open(conf.server.get(
                "pidfile",
                "/tmp/drivel.pid"), "w") as pidfile:
            pidfile.write("%s\n" % os.getpid())

        # Set the signal map
        dctx.signal_map = {
            signal.SIGTERM: lifecycle_cleanup,
            }
        start(conf, options)


def lifecycle_stop(conf, options):
    with open(conf.server.get("pidfile", "/tmp/drivel.pid")) as pidfile:
        pid = pidfile.read()
        try:
            os.kill(int(pid), signal.SIGTERM)
        except Exception, e:
            print >> sys.stderr, "couldn't stop %s" % pid


def findconfig():
    """Try and find a config file."""
    hn = socket.gethostname()

    def pattern(d):
        return "%s/*%s*.conf*" % (d, hn)
    try:
        # Try in current working directory
        return glob(pattern(os.getcwd()))[0]
    except IndexError:
        try:
            # Try in parent dir of this file
            return glob(pattern(os.path.dirname(os.path.dirname(__file__))))[0]
        except IndexError:
            pass

    return None


def main():
    sys.path = sys.path + [os.path.abspath(os.path.dirname(
        os.path.dirname(__file__)))]
    usage = "%prog [options] [start|stop|help]"
    parser = OptionParser(usage=usage)
    parser.add_option('-n', '--name', dest='name',
        help="server name/id.")
    parser.add_option('-c', '--config', dest='config',
        help="configuration file")
    parser.add_option('-s', '--statdump', dest='statdump',
        metavar='INTERVAL', type="int",
        help="dump stats at INTERVAL seconds")
    parser.add_option('-N', '--no-http-logs', dest='nohttp',
        action="store_true",
        help="disable logging of http requests from wsgi server")
    parser.add_option(
        '-D',
        '--no-daemon',
        dest='nodaemon',
        action="store_true",
        help="disable daemonification if specified in config")
    options, args = parser.parse_args()

    if "help" in args:
        parser.print_help()
        sys.exit(0)

    if not options.config:
        options.config = findconfig()
        if options.config:
            print "using %s" % options.config
        else:
            parser.error('please specify a config file')

    sys.path += [os.path.dirname(options.config)]
    conf = config_fromfile(options.config)

    if "start" in args:
        try:
            if conf.server.daemon and not(options.nodaemon):
                lifecycle_start(conf, options)
            else:
                raise AttributeError("no daemon")
        except AttributeError:
            start(conf, options)

    elif "stop" in args:
        lifecycle_stop(conf, options)

if __name__ == '__main__':
    main()
