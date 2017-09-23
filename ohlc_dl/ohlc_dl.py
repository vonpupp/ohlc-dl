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

import csv
import time
import ccxt


class OHLCDownloader:
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

