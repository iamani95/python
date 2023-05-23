import sqlite3 as sql
import csv
import pandas as pd
from dbconnection import db_connection

class ExtractAndTransform:

    def fetch(self,cur):
        # get the sum of each item for each customer
        try:
            self.output_list = []
            sum_each_item = """SELECT c.customer_id, c.age, i.item_name, sum(o.quantity) as quantity
                                FROM customers c
                                INNER JOIN sales s
                                ON c.customer_id = s.customer_id
                                INNER JOIN orders o
                                ON s.sales_id = o.sales_id
                                INNER JOIN items i
                                ON o.item_id = i.item_id
                                WHERE c.age BETWEEN 18 AND 35
                                AND o.quantity IS NOT NULL
                                GROUP BY 1,3;
                                """
            cur.execute(sum_each_item)

            for row in cur.fetchall():
                self.output_list.append(list(row))
            print('INFO: Data fetched successfully from DB.')    
        except Exception as fetch_err:
            print('ERROR: Something went wrong while fetching the record from DB.', fetch_err)        

    def load_data(self, filename):
        # this function loads the python object to the csv file
        try:
            with open(filename, 'wt') as outfile:
                headers = {'Customer', 'Age', 'Item', 'Quantity'}
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow(headers)
                writer.writerows(self.output_list)
            print('INFO: Data loaded successfully into csv file.')    
        except Exception as load_data_err:
            print('ERROR: Something went wrong while loading the data into csv file.', load_data_err)

if __name__ == '__main__':
    con = db_connection()
    cur = con.cursor()
    etl = ExtractAndTransform()
    etl.fetch(cur)
    file_name = 'customer_s30.csv'
    etl.load_data(file_name)
