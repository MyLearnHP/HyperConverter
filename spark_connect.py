from pyspark.sql import SparkSession
import glob
import config as cf
from dict_list import type_
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, escape_name, escape_string_literal, HyperException
import re
import pandas

spark = SparkSession.builder.master("local").appName("DB_test").getOrCreate()


def sparkConnect():
    # fetching DF from spark filestore
    if cf.file_type == 'csv':
        df = spark.read.format(cf.file_type) \
            .option("inferSchema", cf.infer_schema) \
            .option("header", cf.first_row_is_header) \
            .option("sep", cf.delimiter) \
            .load(cf.input_file_path)
        # print('\n', cf.input_file_path, '\n', cf.schema, '\n')

    # fetching table from db from databricks
    elif cf.file_type == 'jdbc':
        df = spark.read.format("jdbc") \
            .option("driver", cf.driver) \
            .option("url", cf.url) \
            .option("dbtable", cf.table) \
            .option("user", cf.user) \
            .option("password", cf.password) \
            .option("inferSchema", cf.infer_schema) \
            .option("header", cf.first_row_is_header) \
            .load()

        df.write.format("csv") \
            .option("enoding", cf.charset) \
            .option("header", cf.first_row_is_header) \
            .option("sep", cf.delimiter) \
            .save('/home/hari/HyperConverter/test')

        # pdf = df.select('*').toPandas()
        # path = '/home/hari/HyperConverter/test.csv'
        # pdf.to_csv(path, sep=',', index=False)

        path = glob.glob('/home/hari/HyperConverter/test/part*.csv')
        cf.input_file_path = path[0]
        cf.input_file_path = path
        print('\n', cf.input_file_path, '\n')

    col = list(df.dtypes)
    print(col)
    print(len(col))
    for i in range(len(col)):
        col[i] = list(col[i])
        col[i][1] = type_[col[i][1]]
    # print('\n', col, '\n')

    x = []
    for i, j in col:
        print(i, j)
        if j == 'varchar':
            max_length = df.agg({i: "max"}).collect()[0]
            #print(max_length)
            xyz = max_length["max({})".format(i)]

            if xyz != None:
                max_length = len(xyz)
                if 19 <= max_length <= 40:
                    max_length = 100
                else:
                    max_length = 30
            else:
                max_length = 35
            print(max_length)
            x.append(TableDefinition.Column(i, SqlType.varchar(max_length), NULLABLE))
        elif j == 'int':
            x.append(TableDefinition.Column(i, SqlType.int(), NULLABLE))
        elif j == 'date':
            x.append(TableDefinition.Column(i, SqlType.date(), NULLABLE))
        elif j == 'numeric':
            x.append(TableDefinition.Column(i, SqlType.numeric(10, 4), NULLABLE))
        elif j == 'bool':
            x.append(TableDefinition.Column(i, SqlType.bool(), NULLABLE))
        elif j == 'big_int':
            x.append(TableDefinition.Column(i, SqlType.big_int(), NULLABLE))
        elif j == 'double':
            x.append(TableDefinition.Column(i, SqlType.double(), NULLABLE))
        elif j == 'text':
            print("this is culprate", i, j)
            x.append(TableDefinition.Column(i, SqlType.text(), NULLABLE))
    print(x)
    print(len(x))
    return x
