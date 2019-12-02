from pyspark.sql import SparkSession
import glob
import config as cf
from dict_list import type_
from tableauhyperapi import TableDefinition, SqlType, NOT_NULLABLE, Nullability, NULLABLE
import re

spark = SparkSession.builder.master("local").appName("DB_test").getOrCreate()


def sparkConnect():
    # fetching DF from spark filestore
    if cf.file_type == 'csv':
        df = spark.read.format(cf.file_type) \
            .option("inferSchema", cf.infer_schema) \
            .option("header", cf.first_row_is_header) \
            .option("sep", cf.delimiter) \
            .load(cf.input_file_path)
        print('\n', cf.input_file_path, '\n', cf.schema, '\n')

    # fetching table from db from databricks
    elif cf.file_type == 'jdbc':
        df = spark.read.format("jdbc") \
            .option("driver", cf.driver) \
            .option("url", cf.url) \
            .option("dbtable", cf.table) \
            .option("user", cf.user) \
            .option("password", cf.password) \
            .option("inferSchema", True) \
            .option("header", True) \
            .load()

        df.write.format("csv") \
            .option("enoding", cf.charset) \
            .save('/home/hari/HyperConverter/test')

        path = glob.glob('/home/hari/HyperConverter/test/part*.csv')
        cf.input_file_path = path[0]
        print('\n', cf.input_file_path, '\n')

    col = df.schema
    col = list(col)
    x = []
    for i in range(len(col)):
        col[i] = str(col[i])
        col[i] = re.split('\(|,', col[i])
        col[i] = col[i][1:-1]
        temp = col[i][1]
        col[i][1] = temp[:-4]

    for i in range(len(col)):
        col[i][1] = type_[col[i][1]]
    print('\n', col, '\n')

    #create list of column schema
    x = []
    for i, j in col:
        df.agg(max(df.columns(j))).show()
        if j == 'varchar':
            x.append(TableDefinition.Column(i, SqlType.varchar(100), NULLABLE))
        elif j == 'int':
            x.append(TableDefinition.Column(i, SqlType.int(), NULLABLE))
        elif j == 'date':
            x.append(TableDefinition.Column(i, SqlType.date(), NULLABLE))
        elif j == 'numeric':
            x.append(TableDefinition.Column(
                i, SqlType.numeric(10, 4), NULLABLE))
        elif j == 'bool':
            x.append(TableDefinition.Column(i, SqlType.bool(), NULLABLE))
        elif j == 'big_int':
            x.append(TableDefinition.Column(i, SqlType.big_int(), NULLABLE))
        elif j == 'double':
            x.append(TableDefinition.Column(i, SqlType.double(), NULLABLE))
    print(x)
    return cf.input_file_path,x
