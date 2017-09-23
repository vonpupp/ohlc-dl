# -*- coding: utf-8 -*-

"""
Copyright (C) 2017 Albert De La Fuente Vigliotti

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
from os import path

# import appdirs
import argparse

import logging
import logging.config

import ConfigParser

import datetime
import dateutil
import dateutil.relativedelta
import calendar

# import click

from ohlc_dl import OHLCDownloader

# -----------------------------------------------------------------------------

# root = os.path.dirname(os.path.dirname(os.path.dirname(
#     os.path.abspath(__file__))))
# sys.path.append(root)

# -----------------------------------------------------------------------------
# common constants

LOGGING_CONFIG_FILE = 'ohlcv-dl.logging.conf'
CONFIG_FILE = 'ohlcv-dl.conf'
CONFIG_LOCATIONS = ['/etc',
                    '/usr/local/etc',
                    # appdirs.user_config_dir('ohlcv-dl'),
                    os.curdir]


class CUIApp:
    """
    App class.
    """

    def __init__(self):
        """
        Creates an empty app.
        Attributes:
        """
        self.ohlcv_downloader = None

    def _parse_args(self):
        """
        Parses and sets up the command line argument
        with config file parsing support
        """
        # Adapted from: https://gist.github.com/wrouesnel/72c237b6e4d76c214a28
        global CONFIG_LOCATIONS, CONFIG_FILE, LOGGING_CONFIG_FILE

        early_parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            add_help=False)
        early_parser.add_argument(
            '--python-debug', help='connect to a python debugging server',
            action='store_true')
        early_parser.add_argument(
            '--config', dest='config',
            help='change default configuration location',
            default=None)
        early_parser.add_argument(
            '--logging-config', dest='logging_config',
            help='change default log file configuration',
            default=None)

        args, remainder_argv = early_parser.parse_known_args()

        # Configure logging as early as possible
        log_locations = [path.join(dir, LOGGING_CONFIG_FILE)
                         for dir in reversed(CONFIG_LOCATIONS)]
        if args.logging_config:
            log_locations.insert(0, args.logging_config)
        loggingConfig = None
        for p in log_locations:
            if path.exists(p):
                loggingConfig = p

        if loggingConfig:
            logging.config.fileConfig(
                loggingConfig, disable_existing_loggers=True)
            logging.info('Loaded logging config from {}'.format(loggingConfig))

        if args.python_debug:
            import pydevd
            pydevd.settrace(suspend=False)
            logging.info('PyDev debugging activated')

        # Override config file defaults if explicitly requested
        if args.config:
            CONFIG_LOCATIONS = [args.config]

        cp = ConfigParser.SafeConfigParser()
        cp.read([path.join(confpath, CONFIG_FILE)
                 for confpath in CONFIG_LOCATIONS])

        # Load config file sections as settings for argparse groups
        if cp is not None:
            for g in self.parser._action_groups:
                # Optional args are general args
                section = None
                if g.title == 'optional arguments':
                    if 'general' in cp._sections:
                        section = cp._sections['general']
                    elif g.title in cp._sections:
                        section = cp._sections[g.title]

                if section is None:
                    continue

                for action in g._actions:
                    for option_string in action.option_strings:
                        if option_string.startswith('--'):
                            if option_string[2:] in section:
                                if action.nargs is not None:
                                    if action.nargs == argparse.ZERO_OR_MORE or\
                                       action.nargs == argparse.ONE_OR_MORE:
                                        val = section[option_string[2:]].split()
                                    else:
                                        val = section[option_string[2:]]
                                else:
                                    val = section[option_string[2:]]

                                if action.type is not None:
                                    if hasattr(val, '__iter__'):
                                        action.default = map(action.type, val)
                                    else:
                                        action.default = action.type(val)
                                else:
                                    action.default = val

        args = self.parser.parse_args(remainder_argv)
        return args

    def parse_config(self):
        """
        """
        # TODO: Ability to continue from current date based on a status file
        # (kind of pid file)?

        self.parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        self.parser.add_argument('-e', '--exchange',
                                 action='store',
                                 dest='exchange',
                                 help='Sets the exchange (default=kraken)',
                                 default='kraken',
                                 type=str)
        # TODO: Think about the logic
        self.parser.add_argument(
            '-t', '--tick',
            action='store',
            dest='tick_interval',
            help='Sets the tick interval in seconds (default=5m)',
            default='5m',
            type=str)
        # TODO: Think about the default value
        self.parser.add_argument(
            '-f', '--from',
            action='store',
            dest='from_timestamp',
            help='Sets the from date',
            type=str)
        # TODO: Think about the default value
        self.parser.add_argument(
            '-u', '--until',
            action='store',
            dest='until_timestamp',
            help='Sets the until date',
            type=str)
        self.parser.add_argument(
            '-p', '--pair',
            action='store',
            dest='symbol',
            help='Sets the currency pair (default=BTC/EUR)',
            default='BTC/EUR',
            type=str)
        self.parser.add_argument(
            '-r', '--rate',
            action='store',
            dest='rate',
            help='Sets the rate in miliseconds (default=500)',
            default='500',
            type=int)
        self.parser.add_argument(
            '-d', '--delay',
            action='store',
            dest='delay',
            help='Sets the delay in miliseconds in case of failure \
                  (default=30000)',
            default='30000',
            type=int)
        self.parser.add_argument(
            '-o', '--outfile',
            action='store',
            dest='outfile',
            help='Sets the out file',
            # default='ohlcvdata.csv',
            type=str)
        self.args = self._parse_args()

    def calculate_period_previous_month(self):
        # Calculate last day of the previous month
        nowutc = datetime.datetime.utcnow()
        previous_period_utc = nowutc +\
            dateutil.relativedelta.relativedelta(months=-1)
        last_day_previous_month = calendar.monthrange(
            previous_period_utc.year,
            previous_period_utc.month)[1]
        from_timestamp_calculated = previous_period_utc.replace(
            day=01,
            hour=00,
            minute=00,
            second=00,
            microsecond=00)
        until_timestamp_calculated = previous_period_utc.replace(
            day=last_day_previous_month,
            hour=23,
            minute=59,
            second=59,
            microsecond=00)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def calculate_period_previous_day_utc(self):
        # Calculate last day of the previous month
        nowutc = datetime.datetime.utcnow()
        previous_period_utc = nowutc +\
            dateutil.relativedelta.relativedelta(days=-1)
        from_timestamp_calculated = previous_period_utc.replace(
            hour=00,
            minute=00,
            second=00,
            microsecond=00)
        until_timestamp_calculated = previous_period_utc.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=00)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def calculate_period_previous_day(self):
        # Calculate last day of the previous month
        now = datetime.datetime.now()
        previous_period = now + dateutil.relativedelta.relativedelta(days=-1)
        from_timestamp_calculated = previous_period.replace(
            hour=00,
            minute=00,
            second=00,
            microsecond=00)
        until_timestamp_calculated = previous_period.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=00)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def run(self):
        """
        App runner (main program)
        """
        self.parse_config()
        # from_timestamp_calculated, until_timestamp_calculated = self.\
        #     calculate_period_previous_month()

        from_timestamp_calculated, until_timestamp_calculated = \
            self.calculate_period_previous_day()
            # self.calculate_period_previous_month()

        # If no arguments, use calculations
        from_timestamp = self.args.from_timestamp
        until_timestamp = self.args.until_timestamp
        if not self.args.from_timestamp:
            from_timestamp = str(from_timestamp_calculated)
        if not self.args.until_timestamp:
            until_timestamp = str(until_timestamp_calculated)

        pattern = '%Y-%m-%d %H:%M:%S'
        from_datetime = datetime.datetime.strptime(from_timestamp, pattern)
        until_datetime = datetime.datetime.strptime(until_timestamp, pattern)

        # If no filename, use template
        if not self.args.outfile:
            outfile = 'dataohlcv.{}.{}.{}.{}.{}.csv'.format(
                self.args.exchange,
                self.args.tick_interval,
                self.args.symbol.translate(None, '/'),
                # from_timestamp.replace(' ', '-'),
                from_datetime.strftime("%Y-%m-%d-%H-%M-%S"),
                # until_timestamp.replace(' ', '-'),
                until_datetime.strftime("%Y-%m-%d-%H-%M-%S"),
                # datetime.datetime.utcnow().strftime("%Y-%m-%d-%H:%M:%S"),
            )

        # Get the data
        self.ohlcv_downloader = OHLCDownloader(outfile)
        self.ohlcv_downloader.download(self.args.exchange,
                                       self.args.tick_interval,
                                       from_timestamp,
                                       until_timestamp,
                                       self.args.symbol,
                                       self.args.rate,
                                       self.args.delay)


##@click.command()
def main():
    # import doctest
    # doctest.testmod()
    app = CUIApp()
    app.run()

# def main(args=None):
#     """Console script for ohlc_dl."""
#     click.echo("Replace this message by putting your code into "
#                "ohlc_dl.cli.main")
#     click.echo("See click documentation at http://click.pocoo.org/")

if __name__ == "__main__":
    main()
