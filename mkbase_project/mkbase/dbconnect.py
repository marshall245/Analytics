#!/usr/bin/env python
import six, sqlalchemy, os, pandas, warnings

__author__ = 'marshall markham'

__all__ = ['PgAuth', 'read_sql', 'dataframe_to_table', 'execute_sql']


# set global defaults
_HOST_DEFAULT = '192.168.0.7'
_DB_DEFAULT = 'sandbox'

# connection class
class PgAuth(object):
    '''used to create a sqlalchmy connection'''
    def __init__(self, host, db, user=None, password=None, port=5432):
        for item in [host, db, user, password]:
            if not isinstance(item, (str, type(None))):
                raise ValueError('non string value passed to one of [host, db, user, password]')
            if not isinstance(port, six.integer_types):
                raise ValueError('port argument expects types int or long')
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
    
    def yeild_engine(self):
        '''create a sql alchemy engine'''
        head = 'postgresql://'
        tail = ''.join(['@', self.host, '/', self.db])

        # test for both user and password provided
        if not (self.user is None or self.password is None):
            jnd = ''.join([head, self.user, ':', self.password, tail])
            return sqlalchemy.create_engine(jnd)

        # if user or password not provided pull from pgpass file
        path2pass = os.path.join(os.environ['HOME'], '.pgpass')
        with open(path2pass, 'r') as f:
            for line in f.readlines():
                line = line.split(':')
                if (line[0] == self.host) and (int(line[1]) == int(self.port)):
                    if self.password is None:
                        password = line[-1].strip()
                    else:
                        password = self.password
                    if self.user is None:
                        user = line[-2].strip()
                    else:
                        user = self.user
                    jnd = ''.join([head, user, ':', password, tail])
                    return sqlalchemy.create_engine(jnd)
        raise RuntimeError('unable to resolve credentials')


# functions for using a postgres database
def read_sql(sql, options=None):
    '''Query a database
    
    parameters
    ----------
    sqlstring : string
        a select statement
    options : PgAuth object
        an object to resolve the network connection        
    '''
    if options is None:
        options = PgAuth(_HOST_DEFAULT, _DB_DEFAULT)
    conn = options.yeild_engine()
    return pandas.read_sql(sql, conn)

def dataframe_to_table(df, schema_table, grant_all, ownerto, options=None, if_exists='fail', index=False, **kwargs):
    '''
    df : pandas.DataFrame
        a dataframe to write as a table
    schema_table : string
        the schema.tablename to write to
    options : PgAuth
        an object for creating the connection
    grant_all : string
        user group or user to use in the GRANT ALL clause
    ownerto : string
        user group or user to use in the alter table owner to clause
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
        - fail: If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.
    index : boolean, default False
        Write DataFrame index as a column.
    dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns.
    '''
    if not isinstance(df, pandas.DataFrame):
        raise RuntimeError('df should be a pandas.DataFrame')
    if options is None:
        options = PgAuth(_HOST_DEFAULT, _DB_DEFAULT)
    conn = options.yeild_engine()

    # set kwarqs dictionary
    kwargs['if_exists'] = if_exists
    kwargs['index'] = index
    tmp = schema_table.split('.')
    if len(tmp) < 2:
        raise RuntimeError('give a schema.table name to 2nd argument')
    name = tmp[1]
    kwargs['schema'] = tmp[0]
    
    # write out
    df.to_sql(name, conn, **kwargs)
    
    # set OWNER TO and GRANT ALL permission
    try:
        pandas.io.sql.execute('ALTER TABLE ' + schema_table + ' OWNER to ' + ownerto, conn)
    except Exception as e:
        warnings.warn('failed to set OWNER on table')
    
    try:
        pandas.io.sql.execute('GRANT ALL ON TABLE ' + schema_table + ' TO ' + grant_all, conn)
    except Exception as e:
        print e
        warnings.warn('failed to GRANT ALL on table')

def execute_sql(sql, options=None, **kwargs):
    '''execute sql against the database specified'''
    if options is None:
        options = PgAuth(_HOST_DEFAULT, _DB_DEFAULT)
    conn = options.yeild_engine()
    return pandas.io.sql.execute(sql, conn)
