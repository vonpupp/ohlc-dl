#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
ohlcv-dl
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

import sys
import os
from os import path

# import appdirs
import argparse

import logging
import logging.config

import ConfigParser

import datetime
import time
import csv
import dateutil
import dateutil.relativedelta
import calendar

# -----------------------------------------------------------------------------

root = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
sys.path.append(root)

# -----------------------------------------------------------------------------

import ccxt  # noqa: E402

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

        early_parser = argparse.ArgumentParser(description=__doc__,
                formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                add_help=False)
        early_parser.add_argument('--python-debug', help='connect to a python debugging server',
                            action='store_true')
        early_parser.add_argument('--config', dest='config',
                            help='change default configuration location',
                            default=None)
        early_parser.add_argument('--logging-config', dest='logging_config',
                            help='change default log file configuration',
                            default=None)

        args,remainder_argv = early_parser.parse_known_args()

        # Configure logging as early as possible
        log_locations = [ path.join(dir, LOGGING_CONFIG_FILE) \
                        for dir in reversed(CONFIG_LOCATIONS) ]
        if args.logging_config:
            log_locations.insert(0, args.logging_config)
        loggingConfig = None
        for p in log_locations:
            if path.exists(p):
                loggingConfig = p

        if loggingConfig:
            logging.config.fileConfig(loggingConfig, disable_existing_loggers=True)
            logging.info('Loaded logging config from {}'.format(loggingConfig))

        if args.python_debug:
            import pydevd
            pydevd.settrace(suspend=False)
            logging.info('PyDev debugging activated')

        # Override config file defaults if explicitly requested
        if args.config:
            CONFIG_LOCATIONS=[ args.config ]

        cp = ConfigParser.SafeConfigParser()
        cp.read([ path.join(confpath, CONFIG_FILE) \
                for confpath in CONFIG_LOCATIONS ])

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
                                    if action.nargs == argparse.ZERO_OR_MORE or \
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

        self.parser = argparse.ArgumentParser(description=__doc__,
                                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        self.parser.add_argument('-e', '--exchange',
                                 action='store',
                                 dest='exchange',
                                 help='Sets the exchange (default=kraken)',
                                 default='kraken',
                                 type=str)
        # TODO: Think about the logic
        self.parser.add_argument('-t', '--tick',
                                 action='store',
                                 dest='tick_interval',
                                 help='Sets the tick interval in seconds (default=5m)',
                                 default='5m',
                                 type=str)
        # TODO: Think about the default value
        self.parser.add_argument('-f', '--from',
                                 action='store',
                                 dest='from_timestamp',
                                 help='Sets the from date (default=<1st last month>)',
                                 type=str)
        # TODO: Think about the default value
        self.parser.add_argument('-u', '--until',
                                 action='store',
                                 dest='until_timestamp',
                                 help='Sets the until date (default=<last day of last month>)',
                                 type=str)
        self.parser.add_argument('-s', '--symbol',
                                 action='store',
                                 dest='symbol',
                                 help='Sets the until date (default=<1st last month>)',
                                 default='BTC/EUR',
                                 type=str)
        self.parser.add_argument('-r', '--rate',
                                 action='store',
                                 dest='rate',
                                 help='Sets the rate in miliseconds (default=500)',
                                 default='500',
                                 type=int)
        self.parser.add_argument('-d', '--delay',
                                 action='store',
                                 dest='delay',
                                 help='Sets the delay in miliseconds in case of failure (default=30000)',
                                 default='10000',
                                 type=int)
        self.parser.add_argument('-o', '--outfile',
                                 action='store',
                                 dest='outfile',
                                 help='Sets the out file (default=ohlcvdata.csv)',
                                 # default='ohlcvdata.csv',
                                 type=str)
        self.args = self._parse_args()

    def calculate_period_previous_month(self):
        # Calculate last day of the previous month
        nowutc = datetime.datetime.utcnow()
        previous_period_utc = nowutc + dateutil.relativedelta.relativedelta(months=-1)
        last_day_previous_month = calendar.monthrange(previous_period_utc.year,
                                                      previous_period_utc.month)[1]
        from_timestamp_calculated = previous_period_utc.replace(day=01,
                                                                hour=00,
                                                                minute=00,
                                                                second=00)
        until_timestamp_calculated = previous_period_utc.replace(day=last_day_previous_month,
                                                                 hour=23,
                                                                 minute=59,
                                                                 second=59)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def calculate_period_previous_day_utc(self):
        # Calculate last day of the previous month
        nowutc = datetime.datetime.utcnow()
        previous_period_utc = nowutc + dateutil.relativedelta.relativedelta(days=-1)
        from_timestamp_calculated = previous_period_utc.replace(hour=00,
                                                                minute=00,
                                                                second=00)
        until_timestamp_calculated = previous_period_utc.replace(hour=23,
                                                                 minute=59,
                                                                 second=59)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def calculate_period_previous_day(self):
        # Calculate last day of the previous month
        now = datetime.datetime.now()
        previous_period = now + dateutil.relativedelta.relativedelta(days=-1)
        from_timestamp_calculated = previous_period.replace(hour=00,
                                                            minute=00,
                                                            second=00)
        until_timestamp_calculated = previous_period.replace(hour=23,
                                                             minute=59,
                                                             second=59)
        return [from_timestamp_calculated, until_timestamp_calculated]

    def run(self):
        """
        App runner (main program)
        """
        self.parse_config()
        # kraken_1m_btcusd_fromdate_untildate_rate
        from_timestamp_calculated, until_timestamp_calculated = self.\
            calculate_period_previous_month()

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
        self.ohlcv_downloader = OhlcvDownloader(outfile)
        self.ohlcv_downloader.download(self.args.exchange,
                                       self.args.tick_interval,
                                       from_timestamp,
                                       until_timestamp,
                                       self.args.symbol,
                                       self.args.rate,
                                       self.args.delay)
        #input_string = self.read_input('input.txt')
        #self._trains.create_graph_from_string(input_string)


def main():
    # import doctest
    # doctest.testmod()
    app = CUIApp()
    app.run()


class OhlcvDownloader:
    """
    Downloader class.
    """

    def __init__(self, out_filename):
        """
        Creates a downloader
        Attributes:
        """
        self._problem = None
        self.out_filehandler = open(out_filename, "wb")
        self.out_csvwriter = csv.writer(self.out_filehandler)

    def init_exchange(self, exchange_name, rate='500'):
        # Parse exchanges
        exchange_args = {'rateLimit': int(rate)}
        if exchange_name.lower() == 'kraken':
            ccxt_exchange_class = ccxt.kraken
        if exchange_name.lower() == 'bittrex':
            ccxt_exchange_class = ccxt.bittrex
        if exchange_name.lower() == 'gdax':
            ccxt_exchange_class = ccxt.gdax
            exchange_args = {'rateLimit': int(rate),
                             'enableRateLimit': True,
                             # 'verbose': True,
                             }

        self.exchange = ccxt_exchange_class(exchange_args)

    def tick_offset(self, tick_interval):
        # Parse tick interval (not pretty, I know, but KISS)
        minute = 60 * 1000
        hour = 60 * minute
        day = 24 * hour
        week = 7 * day
        tick_interval = tick_interval.lower()
        if tick_interval == '1m':
            offset = 1 * minute
        elif tick_interval == '5m':
            offset = 5 * minute
        elif tick_interval == '10m':
            offset = 10 * minute
        elif tick_interval == '15m':
            offset = 15 * minute
        elif tick_interval == '30m':
            offset = 30 * minute
        elif tick_interval == '1h':
            offset = 1 * hour
        elif tick_interval == '4h':
            offset = 4 * hour
        elif tick_interval == '1d':
            offset = 1 * day
        elif tick_interval == '4d':
            offset = 4 * day
        elif tick_interval == '1w':
            offset = 1 * week
        elif tick_interval == '2w':
            offset = 2 * week
        return offset

    def download(self, exchange_name, tick_interval, from_date, until_date,
                 symbol, rate, retry_delay):
        """
        Downloads the ohlcv data
        Attributes:
        """
        # Adapted from: https://github.com/ccxt-dev/ccxt/blob/master/examples/py/fetch-ohlcv-sequentially.py
        msec = 1000

        self.init_exchange(exchange_name, rate)
        offset = self.tick_offset(tick_interval)

        from_timestamp = self.exchange.parse8601(from_date)
        until_timestamp = self.exchange.parse8601(until_date)
        now = self.exchange.milliseconds()

        data = []
        self.out_csvwriter.writerow(['epoch', 'open', 'high',
                                     'low', 'close', 'volume', 'date'])
        while from_timestamp < until_timestamp:
            print('Fetching candles starting from {}'.
                  format(self.exchange.iso8601(from_timestamp)))
            try:
                # each ohlcv candle is a list of:
                # [ timestamp, open, high, low, close, volume ]
                ohlcvs = self.exchange.fetch_ohlcv(symbol,
                                                   tick_interval,
                                                   from_timestamp)
                print('Got {} candles'.
                    format(len(ohlcvs)))

                # don't hit the rateLimit or you will be banned
                time.sleep(self.exchange.rateLimit / msec)

                # Kraken returns 720 candles for 1m timeframe at once
                from_timestamp += len(ohlcvs) * offset
                data += ohlcvs
            except (ccxt.ExchangeError, ccxt.AuthenticationError,
                    ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
                print('Got an error {}, {}. Sleeping for {} seconds'.
                      format(type(error).__name__, error.args,
                             retry_delay / msec))
                time.sleep(retry_delay / msec)

        # Process epoch time into datetime stamp
        for tick in data:
            #tick += datetime.datetime.fromtimestamp(float(tick[0])).strftime('%c')
            timestamp = self.exchange.iso8601(tick[0])
            tick += [timestamp]

        # Write data to csv file
        self.out_csvwriter.writerows(data)


if __name__ == "__main__":
    main()
