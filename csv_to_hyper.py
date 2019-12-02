from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, escape_name, escape_string_literal, HyperException
import config as cf
from spark_connect import sparkConnect

test = sparkConnect()
schema = test[1]
print(schema)

customer_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name=cf.table_name,
    columns=schema
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
                f"(format csv, NULL 'NULL', delimiter ',', header)")

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
