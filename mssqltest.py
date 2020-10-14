import pyodbc
import binascii
import struct

host = 'td1a4dmcslanqnr.c9mc5tzceqtj.us-west-2.rds.amazonaws.com'
port = '1433'

user = 'root'
password = 'baffle123'

database = 'MSSQL_LargeData'
schema = 'New'
table = 'LargeData'
column = 'col_time'

query = "select {} from {}.{}.{};".format(column, database,schema,table)
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';DATABASE='+database+';UID='+user+';PWD='+password)
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
print(data[0])
print(type(data[0]))
'''with open('mssample.txt', 'w') as f:
    for item in data:
        if type(item[0]) is memoryview:
            item = binascii.hexlify(item[0])
            f.write("%s\n" % str(item[0]))
        else:
            f.write("%s\n" % str(item[0]))'''