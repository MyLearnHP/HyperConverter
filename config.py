driver = "com.simba.spark.jdbc41.Driver"
access_token = "dapi32ec1c4c866c8bac4b8a0be2b417b326"
url = "jdbc:spark://dbc-7461a053-3df1.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/4471256816674954/1206-115553-grins359;AuthMech=3;UID=token;PWD=" + access_token

user = "hari.prasad@d3analytics.com"
password = "Devil@1998"

charset = 'UTF8'

db_name = "market_scan"
table_name = "condition_occurrence"

table = db_name + '.' + table_name

file_type = "jdbc"
infer_schema = True
first_row_is_header = True
delimiter = ","

schema = ''

input_file_path = '/home/hari/HyperConverter/' + table_name + '.csv'
hyper_file_path = table_name + '.hyper'
