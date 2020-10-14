import mysql.connector
import binascii

host = 'ec2-44-234-147-210.us-west-2.compute.amazonaws.com'
port = '8444'

user = 'root'
password = 'baffle123'

database = 'New'
table = 'LargeData'
column = 'col_time'

query = 'select {} from {}.{};'.format(column, database, table)
#print(query)
connection = mysql.connector.connect(host=host,database=database,user=user,password=password,port=port)
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
print(data[0])
print(type(data[0]))
'''with open('mysample.txt', 'w') as f:
    for item in data:
        if type(item[0]) is memoryview:
            item = binascii.hexlify(item[0])
            if item[0] == 1:
                f.write("%s\n" % str(True))
            elif item[0] == 0:
                f.write("%s\n" % str(False))
            else:
                f.write("%s\n" % str(None))
        else:
            if item[0] == 1:
                f.write("%s\n" % str(True))
            elif item[0] == 0:
                f.write("%s\n" % str(False))
            else:
                f.write("%s\n" % str(None))'''