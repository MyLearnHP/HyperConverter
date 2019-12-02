driver = "com.simba.spark.jdbc41.Driver"
access_token = "dapicd9f883d057b8971d40343be600cc992"
url = "jdbc:spark://dbc-7461a053-3df1.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/4471256816674954/1122-120031-fry34;AuthMech=3;UID=token;PWD=" + access_token

user = "hari.prasad@d3analytics.com"
password = "Devil@1998"

charset = 'UTF8'

db_name = "market_scan"
table_name = "Sample Data"

table = db_name + '.' + table_name

file_type = "csv"
infer_schema = True
first_row_is_header = True
delimiter = ","

schema = ''

input_file_path = '/home/hari/HyperConverter/' + table_name + '.csv'
hyper_file_path = table_name + '.hyper'
