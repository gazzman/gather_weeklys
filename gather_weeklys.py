#!/usr/bin/python
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
import argparse

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import Column, Date, String
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.dialects.postgresql import VARCHAR

STARTPATTERN = 'LIST OF AVAILABLE WEEKLYS OPTIONS'
HEADERPATTERN = 'Ticker Symbol'

EXPCOLHEADERS = ['expiry_0', 'expiry_1', 'expiry_2',
                 'expiry_3', 'expiry_4', 'expiry_5']

def gen_table(tablename, metadata, schema=None):
    table = Table(tablename, metadata,
                  Column('ticker', VARCHAR(21), index=True, primary_key=True),
                  Column('name', String),
                  Column('type', String),
                  Column('list_date', Date, index=True, primary_key=True),
                  Column(EXPCOLHEADERS[0], Date),
                  Column(EXPCOLHEADERS[1], Date),
                  Column(EXPCOLHEADERS[2], Date),
                  Column(EXPCOLHEADERS[3], Date),
                  Column(EXPCOLHEADERS[4], Date),
                  Column(EXPCOLHEADERS[5], Date),
                  schema=schema)
    return table

def bar_to_db(conn, table, symbol, timestamp, bar):
    try:
        conn.execute(table.insert(), ticker=symbol, timestamp=timestamp, **bar)
    except IntegrityError as err:
        if 'duplicate key' in str(err): pass
        else: raise(err)

if __name__ == "__main__":
    description = 'A utility for storing the CBOE\'s weeklies in a database.'
    filename_help = 'the filename of the xls spreasheet of available weeklies'
    db_help = 'the name of a postgresql database'
    tbl_help = 'the name of the table in which to store the data'
    schema_help = 'an optional database schema'
    host_help = 'the host on which the db lives'
    
    p = argparse.ArgumentParser(description=description)
    p.add_argument('filename', type=str, help=filename_help)
    p.add_argument('database', help=db_help)
    p.add_argument('--host', default='', help=host_help)
    p.add_argument('--schema', help=schema_help)
    p.add_argument('--tablename', default='available_weeklies', help=tbl_help)
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
                                                                tablename)

    # Parse the xls table into a list
    wb = xlrd.open_workbook(args.file)
    sh = wb.sheet_by_index(0)
    table = [sh.row_values(x) for x in xrange(sh.nrows)]
    starts = [x for x in table if STARTPATTERN in x[0]]
    if len(starts) < 1: raise Exception('No list detected with %s' 
                                        % STARTPATTERN)

    # Separate the table by week    
    sidxs = [table.index(x) for x in starts]
    weeks = []
    while len(sidxs) > 0:
        weeks.append(table[sidxs[-1]:])
        table = table[0:sidxs[-1]]
        sidxs = sidxs[:-1]

    # For each week, write the data to the database
    for week in weeks:
        header = [x for x in week if HEADERPATTERN in x[0]][0]
        if len(header) < 1: raise Exception('No header detected with %s'
                                            % HEADERPATTERN)
        start = week.index(header)
        datarows = [x for x in week[start:] if x[0].strip() != '']
        expiry_dates = [str(x).split('.')[0] for x in header[-6:]]
        for row in datarows:
            expirys = row[-6:]
            for i in xrange(len(expirys)):
                if expirys[i].strip() == '': expiry[i] = None
                else: expirys[i] == expiry_dates[i]
            expdata = dict(zip(EXPCOLHEADERS, expirys))
            try:
                conn.execute(table.insert(), ticker=row[0], 
                                                name=row[1],
                                                type=row[2],
                                                list_date=row[3],
                                                **expdata)
            except IntegrityError as err:
                if 'duplicate key' in str(err): pass
                else: raise(err)

    print >> sys.stderr, "Data written."