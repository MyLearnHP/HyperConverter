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
print(len(col))
for i in range(len(col)):
    col[i] = list(col[i])
    col[i][1] = type_[col[i][1]]
print(col, len(col))

xyz = [

        TableDefinition.Column('cdl_uuid', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('cdl_frequency', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('cdl_effective_date', SqlType.date(), NULLABLE),
        TableDefinition.Column('cdl_run_identifier', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('customer_id', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('customer_type', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('customer_name', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('customer_address', SqlType.varchar(100), NULLABLE),
        TableDefinition.Column('city', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('state', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('zip', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('phone', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('fax', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('email', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('customer_status', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('metropolitan_statistical_area_name', SqlType.varchar(100), NULLABLE),
        TableDefinition.Column('blocked_account', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('phs', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_group_code', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_group_description', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_type_code', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_type_description', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_subtype_code', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_subtype_description', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_service_code', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('party_service_description', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('br_segment', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('br_sub_segment', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('channel_name', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('market_normalized_name', SqlType.varchar(100), NULLABLE),
        TableDefinition.Column('brand_normalized_name', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('form_strength_normalized_name', SqlType.varchar(35), NULLABLE),
        TableDefinition.Column('normalized_name', SqlType.varchar(35), NULLABLE),
        TableDefinition.Column('competitor_flag', SqlType.bool(), NULLABLE),
        TableDefinition.Column('year', SqlType.int(), NULLABLE),
        TableDefinition.Column('semester_number', SqlType.int(), NULLABLE),
        TableDefinition.Column('quarter_number', SqlType.int(), NULLABLE),
        TableDefinition.Column('month_number', SqlType.int(), NULLABLE),
        TableDefinition.Column('week_number', SqlType.int(), NULLABLE),
        TableDefinition.Column('transaction_timestamp', SqlType.date(), NULLABLE),
        TableDefinition.Column('ddd_source_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('ddd_source_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('ddd_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('ddd_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('ddd_dollars', SqlType.numeric(10,4), NULLABLE),
        TableDefinition.Column('ddd_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('ddd_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('ddd_normalized_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('ddd_normalized_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('chargeback_source_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('chargeback_source_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('chargeback_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('chargeback_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('chargeback_dollars', SqlType.numeric(10,4), NULLABLE),
        TableDefinition.Column('chargeback_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('chargeback_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('chargeback_normalized_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('chargeback_normalized_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('self_reported_source_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_source_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('self_reported_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('self_reported_dollars', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_normalized_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('self_reported_normalized_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('integrated_units', SqlType.int(), NULLABLE),
        TableDefinition.Column('integrated_dollars', SqlType.numeric(10,3), NULLABLE),
        TableDefinition.Column('integrated_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('integrated_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('integrated_normalized_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_source_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_source_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('867_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('867_dollars', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_normalized_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('867_normalized_units_uom', SqlType.varchar(30), NULLABLE),
        TableDefinition.Column('phs_units', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('phs_dollars', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('phs_dot', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('phs_mcg', SqlType.numeric(10,5), NULLABLE),
        TableDefinition.Column('phs_normalized_units', SqlType.numeric(10,5), NULLABLE)]
print(len(xyz))
print(len(col))
x = []
for i, j in col:

    if j == 'varchar':
        max_length = df.agg({i: "max"}).collect()[0]
        # print(max_length)
        temp = max_length["max({})".format(i)]

        if xyz != None:
            max_length = len(temp)
            if 19 <= max_length <= 40:
                max_length = 100
            else:
                max_length = 30
        else:
            max_length = 35

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
        x.append(TableDefinition.Column(i, SqlType.numeric(10, 4), NULLABLE))
    else:
        x.append(TableDefinition.Column(i, SqlType.text(), NULLABLE))
    print(i, j, max_length)
    print(x[len(x)-1], len(x))
print(len(x))

table = TableDefinition(
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

            connection.catalog.create_table(table_definition=table)
            # `execute_command` executes a SQL statement and returns the impacted row count.
            count_in_customer_table = connection.execute_command(
                command=f"COPY {table.table_name} from {escape_string_literal(cf.input_file_path)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header)")

            print(
                f"The number of rows in table {table.table_name} is {count_in_customer_table}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
