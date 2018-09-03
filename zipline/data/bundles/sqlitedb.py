#
# Ingest stock sqlite databases to create a zipline data bundle

import os

import numpy  as np
import pandas as pd
from trading_calendars import register_calendar_alias
import requests
import sqlite3

from . import core as bundles


boDebug = True  # Set True to get trace messages

from zipline.utils.cli import maybe_show_progress

def _cachpath(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))

IFIL = "F:\code\stock\db\stock.db"
conn = sqlite3.connect(IFIL, check_same_thread=False)


def sqlitedb_equities(symbols):
    return SqlitedbBundle(symbols).ingest


def load_splits_and_dividends():

    splits = {}
    dividends = {}

    query = 'select symbol,date,fenhong,songzhuangu from divs_splits where category=1'
    data = conn.cursor().execute(query)
    for row in data:
        symbol = row[0]
        date = row[1]
        pxVal = row[2]
        sgVal = row[3]
        if sgVal != 0:
            if symbol not in splits.keys():
                splits[symbol] = []
            splits[symbol].append({
                    'effective_date' : date,
                    'ratio' : 10 / (10 + sgVal),
                })

        if pxVal != 0:
            if symbol not in dividends.keys():
                dividends[symbol] = []
            dividends[symbol].append({
                'ex_date' : date,
                'amount' : pxVal / 10,
            })

    return splits, dividends


def zipline_splits_and_dividends(symbol_map):
    raw_splits, raw_dividends = load_splits_and_dividends()
    splits = []
    dividends = []
    for symbol, sid in symbol_map.iteritems():
        if symbol in raw_splits:
            split = pd.DataFrame(data=raw_splits[symbol])
            split['sid'] = sid
            split.index = split['effective_date'] = pd.DatetimeIndex(split['effective_date'])
            splits.append(split)
        if symbol in raw_dividends:
            dividend = pd.DataFrame(data = raw_dividends[symbol])
            dividend['sid'] = sid
            dividend['record_date'] = dividend['declared_date'] = dividend['pay_date'] = pd.NaT
            dividend.index = dividend['ex_date'] = pd.DatetimeIndex(dividend['ex_date'])
            dividends.append(dividend)
    return splits, dividends


class SqlitedbBundle:

    def __init__(self, symbols):
        self.symbols = symbols

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        sqlitedb_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir,
                      self.symbols)

@bundles.register("sqlitedb")
def sqlitedb_bundle(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               symbols):
    if boDebug:
        print ("entering ingest and creating blank metadata")
    if False == os.path.exists(IFIL):
        print("DB source file %s not exist in current path:" % IFIL)
        raise IOError
    if len(symbols) == 0:
        query = "select symbol as name from stock"
        _df = pd.read_sql(query, conn)
        for table in _df.name:
            symbols[table] = None
    if boDebug:
        print("total symbols tuSymbols=", tuple(symbols))

    metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('symbol', 'object'),
    ]))

    if boDebug:
        print ("metadata", type(metadata))
        print (metadata.describe)


    daily_bar_writer.write(_pricing_iter(symbols, metadata, show_progress, cache ,calendar), show_progress=True)

    metadata['exchange'] = "sqlitedb"

    if boDebug:
        print ("returned from daily_bar_writer")
        print ("calling asset_db_writer")
        print ("metadata", type(metadata))

    # Not sure why symbol_map is needed
    symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)
    if boDebug:
        print ("symbol_map", type(symbol_map))
        print (symbol_map)

    asset_db_writer.write(equities=metadata)

    if boDebug:
        print ("returned from asset_db_writer")
        print ("calling adjustment_writer")

    splits, dividends = zipline_splits_and_dividends(symbol_map)

    splits_df = pd.concat(splits, ignore_index=True)
    dividends_df = pd.concat(dividends, ignore_index=True)

    adjustment_writer.write(
      splits=splits_df,
      dividends=dividends_df,
    )

    if boDebug:
        print ("returned from adjustment_writer")
        print ("now leaving ingest function")
    conn.close()

def _pricing_iter(symbols, metadata, show_progress ,cache, calendar):
    sid = 0
    with maybe_show_progress(
            symbols,
            show_progress,
            label='Fetch stocks pricing data from db: ') as it, \
            requests.Session() as session:
        for symbol in it:
            path = _cachpath(symbol, 'ohlcv')
            try:
                df = cache[path]
            except KeyError:
                query = "select stat_date date,open,high,close,vol volume from stock_history where symbol='%s' order by date desc" % symbol
                df = cache[path]  = pd.read_sql(sql=query, con=conn, index_col='date', parse_dates=['date']).sort_index()
                if boDebug:
                    print ("read_sqllite df", type(df), "length", len(df))

            start_date = df.index[0]
            end_date = df.index[-1]

            ac_date = end_date + pd.Timedelta(days=1)
            if boDebug:
                print ("start_date", type(start_date), start_date)
                print ("end_date", type(end_date), end_date)
                print ("ac_date", type(ac_date), ac_date)

            metadata.iloc[sid] = start_date, end_date, ac_date, symbol
            new_index = ['open', 'high', 'low', 'close', 'volume']
            df = df.reindex(columns = new_index, copy=False)

            sessions = calendar.sessions_in_range(start_date, end_date)
            df = df.reindex(
                sessions.tz_localize(None),
                copy=False,
                ).fillna(0.0)

            yield sid, df
            sid += 1

register_calendar_alias("SQLITEDB", "SHSZ")
