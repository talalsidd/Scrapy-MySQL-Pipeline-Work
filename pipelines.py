# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import mysql.connector as msql
from mysql.connector import Error


class ZmapsPipeline(object):
    def __init__(self):
        self.create_connection()
        self.create_table()
    def create_connection(self):
        try:
            self.conn = msql.connect(host='localhost', user='[user]',
                                password='[password]',database='[database]')  # give ur username, password
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()
                self.cursor.execute("CREATE DATABASE IF NOT EXISTS [database]")
                print("Database is created")
        except Error as e:
            print("Error while connecting to MySQL", e)

    def create_table(self):
        self.conn = msql.connect(host='localhost', user='[user]',
                                password='[password]',database='[database]')
        self.cursor = self.conn.cursor()
        #self.cursor.execute('DROP TABLE IF EXISTS [table_name];')
        # in the below line please pass the create table statement which you want #to create
        self.cursor.execute("CREATE TABLE IF NOT EXISTS [table_name](URL_ID varchar(20) NOT NULL,URL varchar(400) NOT NULL, LAST_CRAWLED datetime NOT NULL DEFAULT current_timestamp())")

    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self,item):
        self.cursor.execute("""INSERT INTO [table_name](URL_ID,URL,LAST_CRAWLED) VALUES (%s,%s,%s)""",(
            item['url_id'],
            item['url'],
            item['crawl_time']
        ))
        self.conn.commit()
