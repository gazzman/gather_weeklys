#!/usr/bin/python
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
import argparse
import sys

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import Column, Date, String
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.dialects.postgresql import VARCHAR
import xlrd

STARTPATTERN = 'LIST OF AVAILABLE WEEKLYS OPTIONS'
HEADERPATTERN = 'Ticker Symbol'
EXPCOLNUM = 6

def gen_table(tablename, metadata, schema=None):
    expcols = [Column('expiry_%i' % i, Date) for i in xrange(0, EXPCOLNUM)]
    return Table(tablename, metadata,
                 Column('ticker', VARCHAR(21), index=True, primary_key=True),
                 Column('name', String),
                 Column('type', String),
                 Column('list_date', Date, index=True, primary_key=True),
                 *expcols,
                 schema=schema)

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
    wb = xlrd.open_workbook(args.filename)
    sh = wb.sheet_by_index(0)
    data = [sh.row_values(x) for x in xrange(sh.nrows)]
    starts = [x for x in data if STARTPATTERN in x[0]]
    if len(starts) < 1: raise Exception('No list detected with %s' 
                                        % STARTPATTERN)

    # Separate the table by week    
    sidxs = [data.index(x) for x in starts]
    weeks = []
    while len(sidxs) > 0:
        weeks.append(data[sidxs[-1]:])
        data = data[0:sidxs[-1]]
        sidxs = sidxs[:-1]

    # For each week, write the data to the database
    for week in weeks:
        header = [x for x in week if HEADERPATTERN in x[0]][0]
        if len(header) < 1: raise Exception('No header detected with %s'
                                            % HEADERPATTERN)
        start = week.index(header)
        datarows = [x for x in week[start+1:] if x[0].strip() != '']
        expiry_dates = [str(x).split('.')[0] for x in header[-6:]]
        for row in datarows:
            expirys = row[-6:]
            for i in xrange(len(expirys)):
                if expirys[i].strip() == '': expirys[i] = None
                else: expirys[i] = expiry_dates[i]
            expdata = dict(zip(EXPCOLHEADERS, expirys))
            tick = row[0].replace('*', '')
            tick = tick.replace(' ', '')
            ld = str(row[3]).split('.')[0]
            try:
                conn.execute(table.insert(), ticker=tick, 
                             name=row[1].replace('*',''), 
                             type=row[2].replace('*',''), list_date=ld, 
                             **expdata)
                print >> sys.stderr, "Writing %s for %s" % (tick, ld)
            except IntegrityError as err:
                if 'duplicate key' in str(err): 
                    conn.execute(table.update().where(table.c.ticker==tick)\
                                               .where(table.c.list_date==ld),
                                 ticker=tick, name=row[1].replace('*',''),
                                 type=row[2].replace('*',''), list_date=ld,
                                 **expdata)
                    print >> sys.stderr, "Updated %s for %s" % (tick, ld)
                else: raise(err)

    print >> sys.stderr, "Data written."
