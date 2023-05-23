import sqlite3 as sql

def db_connection():
    # this function returns a db con and engine object
    try:
        con = sql.connect('S30 ETL Assignment.db')
        print('INFO: DB Connected Successfully.')
        return con
    except Exception as db_err:
        print('ERROR: Something went wrong while connecting to DB:', db_err)
    