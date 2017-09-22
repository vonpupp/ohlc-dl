# OHLC Downloader

Under construction!

## Setup

```shell
pip install -r requirements.txt
```

## Example of use

```shell
python2 ohlc-dl.py -e kraken -t 1m -f '2017-09-01 00:00:00' -u '2017-09-05 00:00:00' -s 'BTC/EUR'
python2 ohlc-dl.py -e gdax -t 1m -f '2017-01-01 00:00:00' -u '2017-09-20 00:00:00' -s 'BTC/EUR'
python2 ohlc-dl.py -e bittrex -t 1m -f '2017-09-01 00:00:00' -u '2017-09-05 00:00:00' -s 'BTC/USDT'
```
