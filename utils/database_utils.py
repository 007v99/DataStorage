import pymysql
import pandas as pd
import numpy as np


class Database():
    def __init__(self, root, password, database_name):
        """
        初始化
        """
        self.connection = pymysql.connect(
            host='localhost',  # 主机名
            user=root,  # 用户名
            password=password,  # 密码
            charset='utf8mb4',  # 字符集
            cursorclass=pymysql.cursors.DictCursor  # 指定游标类型为字典类型
        )
        with self.connection.cursor() as cursor:
            # 检查数据库是否存在
            cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            result = cursor.fetchone()
    
            # 如果数据库不存在，则创建
            if not result:
                print(f"Database '{database_name}' does not exist. Creating it...")
                cursor.execute(f"CREATE DATABASE {database_name}")
            else:
                print(f"Database '{database_name}' already exists.")
            
        # 选择数据库
        self.connection.select_db(database_name)
        print(f"Successfully selected the database '{database_name}'")
        # 创建游标
        self.cursor = self.connection.cursor()


    def create_table(self, table_name, columns):
        """
        建表
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
            self.connection.commit()
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
    

    def insert_data(self, table_name, data):
        """
        插入数据
        data: dict
        (%(user_id)s, %(name)s, %(age)s)
        """
        try:
            with self.connection.cursor() as cursor:
                placeholders = ', '.join([f'%({k})s' for k in data.keys()])
                keys = ', '.join(data.keys())
                sql = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
                cursor.execute(sql, data)
            self.connection.commit()
        except Exception as e:
            print(f"Error inserting data into table '{table_name}': {e}")


    def free(self):
        """
        释放连接
        """
        self.connection.close()