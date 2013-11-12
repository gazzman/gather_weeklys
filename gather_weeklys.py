#!/usr/bin/python
__version__ = ".01"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
from datetime import datetime
import argparse
import re
import sys

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import Column, Date, String
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.dialects.postgresql import VARCHAR
from xlrd import open_workbook, xldate_as_tuple

STARTPATTERN = re.compile('LIST OF AVAILABLE WEEKLY[S]? OPTIONS'.lower())
EXPCOLNUM = 7

# adjust depending on spreadsheet headers
DBCOLHEAD = {'ticker': 'ticker',
             'name': 'name',
             'type': 'product_type',
             'list_date': 'list_date',
            }

def gen_table(tablename, metadata, schema=None):
    expcols = [Column('expiry_%i' % i, Date) for i in xrange(0, EXPCOLNUM)]
    return Table(tablename, metadata,
                 Column(DBCOLHEAD['ticker'], VARCHAR(21), index=True, primary_key=True),
                 Column(DBCOLHEAD['name'], String),
                 Column(DBCOLHEAD['type'], String),
                 Column(DBCOLHEAD['list_date'], Date, index=True, primary_key=True),
                 *expcols,
                 schema=schema)

def parse_expiry_data(week, first_header_pattern='ticker', datemode=0):
    headrow = [ row for row in week if first_header_pattern in row[0].lower() ]
    assert len(headrow) == 1
    headrowidx = week.index(headrow[0])
    expirys = [ [ y for y in x if y != '' ] for x in week[1:headrowidx] if str(x) != '']
    expiry_data = [ (x[0], [ datetime(*xldate_as_tuple(y, datemode)).date().isoformat() for y in x[1:] ]) 
                    for x in expirys if len(x) > 0 
                  ]
    data = [ dict(zip(headrow[0], row)) for row in week[headrowidx:] ]
    return dict(expiry_data), data

def gen_dbrow(expirys, rowdict):
    dbrow = dict([ (c, rowdict[k]) for c in DBCOLHEAD.values() for k in rowdict
                                   if c in k.strip().lower().replace(' ', '_') ])
    dbrow.update(expirys)
    dbrow[DBCOLHEAD['list_date']] = datetime.strptime(str(dbrow[DBCOLHEAD['list_date']])\
                                            .split('.')[0], '%Y%m%d').date().isoformat()
    for k in DBCOLHEAD:
        dbrow[DBCOLHEAD[k]] = dbrow[DBCOLHEAD[k]].replace('*', '').strip()
    return dbrow


if __name__ == "__main__":
    description = 'A utility for storing the CBOE\'s weeklys in a database.'
    filename_help = 'the filename of the xls spreasheet of available weeklys'
    db_help = 'the name of a postgresql database'
    tbl_help = 'the name of the table in which to store the data.'
    tbl_help += ' defaults to \'available_weeklys\''
    schema_help = 'an optional database schema'
    host_help = 'the host on which the db lives'
    
    p = argparse.ArgumentParser(description=description)
    p.add_argument('filename', type=str, help=filename_help)
    p.add_argument('database', help=db_help)
    p.add_argument('--host', default='', help=host_help)
    p.add_argument('--schema', help=schema_help)
    p.add_argument('--tablename', default='available_weeklys', help=tbl_help)
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)
    args = p.parse_args()

    # Establish connection to db
    dburl = 'postgresql+psycopg2://%s/%s' % (args.host, args.database)
    engine = create_engine(dburl)
    conn = engine.connect()
    print >> sys.stderr, "Connected to db %s" % args.database

    # Create table and schema if necessary
    if args.schema:
        try: engine.execute(CreateSchema(args.schema))
        except ProgrammingError: pass
    metadata = MetaData(engine)
    table = gen_table(args.tablename, metadata, schema=args.schema)
    metadata.create_all()
    print >> sys.stderr, "Preparing to write to table %s.%s" % (args.schema,
                                                                args.tablename)

    # Parse the xls data into a list
    wb = open_workbook(args.filename)
    sh = wb.sheet_by_index(0)
    data = [sh.row_values(x) for x in xrange(sh.nrows)]
    starts = [x for x in data if STARTPATTERN.search(x[0].lower())]
    if len(starts) < 1: 
        raise Exception('No list detected in %s' % args.filename)

    # Separate the table by week    
    sidxs = [data.index(x) for x in starts]
    weeks = []
    while len(sidxs) > 0:
        weeks.append(data[sidxs[-1]:])
        data = data[0:sidxs[-1]]
        sidxs = sidxs[:-1]

    for week in weeks:
        expiry_data, rowdicts = parse_expiry_data(week, datemode=wb.datemode)
        for rowdict in rowdicts:
            weekly_type = [ k for k, v in rowdict.items() if str(v).lower() == 'x' ]
            expirys = [ v for k, v in expiry_data.items() for w in weekly_type if w.lower() in k.lower() ]
            while len(expirys) > 1:
                    expirys[0] += expirys[-1]
                    expirys = expirys[:-1]
            try:
                expirys = list(set(expirys[0]))
                expirys.sort()
                expirys = zip(['expiry_%i' % i for i in xrange(0, EXPCOLNUM)], expirys)
                dbrow = gen_dbrow(expirys, rowdict)
                try:
                    conn.execute(table.insert(), **dbrow)
                    print >> sys.stderr, "Writing %s for %s" % (dbrow[DBCOLHEAD['ticker']],
                                                                dbrow[DBCOLHEAD['list_date']])
                except IntegrityError as err:
                    if 'duplicate key' in str(err): 
                        conn.execute(table.update().where(table.c.ticker==dbrow[DBCOLHEAD['ticker']])\
                                                   .where(table.c.list_date==dbrow[DBCOLHEAD['list_date']]),
                                     **dbrow)
                        print >> sys.stderr, "Updating %s for %s" % (dbrow[DBCOLHEAD['ticker']],
                                                                    dbrow[DBCOLHEAD['list_date']])
                    else: raise(err)
            except IndexError:
                pass
