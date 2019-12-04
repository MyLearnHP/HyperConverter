from pyspark.sql import SparkSession
import glob
from pathlib import Path
import config as cf
from dict_list import type_
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, escape_name, escape_string_literal, HyperException


spark = SparkSession.builder.master("local").appName("DB_test").getOrCreate()


df = spark.read.format(cf.file_type) \
            .option("inferSchema", cf.infer_schema) \
            .option("header", cf.first_row_is_header) \
            .option("sep", cf.delimiter) \
            .load(cf.input_file_path)


col = list(df.dtypes)

for i in range(len(col)):
    col[i] = list(col[i])
    col[i][1] = type_[col[i][1]]


x = []
for i, j in col:
    if j == 'varchar':
        max_length = df.agg({i: "max"}).collect()[0]
        # print(max_length)
        xyz = max_length["max({})".format(i)]

        if xyz != None:
            max_length = len(xyz)
            if 19 <= max_length <= 40:
                max_length = 100
            else:
                max_length = 30
        else:
            max_length = 35

        print(i, j, max_length)
        x.append(TableDefinition.Column(i, SqlType.varchar(max_length + 1), NULLABLE))
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

customer_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name=cf.table_name,
    columns=x
)

def run_create_hyper_file_from_csv():
    """
    Loading data from a csv into a new Hyper file
    """
    print("Load data from CSV into table in new Hyper file")
    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        with Connection(endpoint=hyper.endpoint,
                        database=cf.hyper_file_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_table(table_definition=customer_table)
            # `execute_command` executes a SQL statement and returns the impacted row count.
            count_in_customer_table = connection.execute_command(
                command=f"COPY {customer_table.table_name} from {escape_string_literal(cf.input_file_path)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header, quote '\"', escape '\\')")

            print(
                f"The number of rows in table {customer_table.table_name} is {count_in_customer_table}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
