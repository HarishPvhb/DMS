import mysql.connector
import psycopg2
import cx_Oracle
import pyodbc
from collections import defaultdict
import sys, os
import filecmp
import binascii
import time
import csv
import struct

# parameters to be passed to this script.

bs_host = 'ec2-44-234-147-210.us-west-2.compute.amazonaws.com'
src_db_host = 'td1a4dmcslanqnr.c9mc5tzceqtj.us-west-2.rds.amazonaws.com'
dest_db_host = 'lalit-mysql-aurora.cluster-c9mc5tzceqtj.us-west-2.rds.amazonaws.com'

bs_port = '8444'
src_db_port = '1433'
dest_db_port = '3306'

root_user = 'root'
root_pass = 'baffle123'

BPS_path = 'BPS2.txt'
BES_path = ' '

res_dir = 'C:/Users/haris/Desktop/DMS/BPS2-1.txt'

csv_path = 'sample2.csv'

def create_log_dirs():
    '''
    Creating log dir to store temp and log files.
    '''
    log_dir = '/tmp/workload_logs'
    decryption_logs =  log_dir + '/decryption_logs'

    os.system('mkdir -p {}'.format(log_dir))
    os.system('mkdir -p {}'.format(decryption_logs))

    return log_dir, decryption_logs


def parse_CSV(csv_path, column):
    
    with open('sample.csv', 'r') as f:
        data = csv.reader(f)

        for line in data:
            db = line[0].split('.')[0]
            schema = line[0].split('.')[1]
            table = line[0].split('.')[2]
            count = line[1]
            print(line[0],' : ',count)
            print(db,table,schema)


def parse_BPS(file_path):
    '''
    Returns db_name.table_name.col_name. and datatype of the column
    '''
    table_columns_dict = defaultdict(set)
    column_type_dict = {}

    with open(file_path, 'r', encoding='utf-8', errors='replace') as fp:
        lines = fp.readlines()

    for line in lines:
        table = ".".join(line.split(' ')[0].split('.')[:-1])
        column = line.split(' ')[0].split('.')[-1]

        table_columns_dict[table].add(column)

        column_type_dict[column] = line.split(' ')[2].strip('\n')

    return table_columns_dict, column_type_dict


def get_connection(host, user, password, port, database):
    '''
    Returns connection object as per the DB used.
    '''
    db_type = None
    connection = None

    if port == 8444:
        db_type = get_db_type(int(dest_db_port))
    else:
        db_type = get_db_type(port)

    if db_type == 'mysql':
        connection = mysql.connector.connect(host=host,
                                             database=database,
                                             user=user,
                                             password=password,
                                             use_unicode=False,
                                             port=port)
    
    elif db_type == 'postgres':
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port)

    elif db_type == 'sql-server':
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';DATABASE=MSSQL_LargeData;UID='+user+';PWD='+password)
    
    elif db_type == 'oracle':
        dsn = cx_Oracle.makedsn(host=host, 
                                port=port, 
                                service_name='orcl')
        connection = cx_Oracle.connect(user=user,
                                       password=password,
                                       dsn=dsn)

    return connection


def run_query(host, port, root_user, root_pass, query, database=None):
    '''
    '''
    retry = 6
    while(retry):
        try:
            connection = get_connection(host, root_user, root_pass, int(port), database)
            
            cursor = connection.cursor()
            cursor.execute(query)

            data = cursor.fetchall()

            return data

        except Exception as e:
            print("RETRYING...\nQUERY: {}".format(query))
            time.sleep(5)
            retry -= 1
            continue

    if retry == 0:
        print("Error in Validating Data. EXITING.\nERROR:{}".format( e ))
        sys.exit(1)


def run_query_datetimeoffset(host, port, root_user, root_pass, query, database=None):
    '''
    '''
    def handle_datetimeoffset(dto_value):
        tup = struct.unpack("<6hI2h", dto_value)
        tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)
    retry = 6
    while(retry):
        try:
            connection = get_connection(host, root_user, root_pass, int(port), database)
            
            cursor = connection.cursor()
            connection.add_output_converter(-155, handle_datetimeoffset)
            cursor.execute(query)

            data = cursor.fetchall()

            return data

        except Exception as e:
            print("RETRYING...\nQUERY: {}".format(query))
            time.sleep(5)
            retry -= 1
            continue

    if retry == 0:
        print("Error in Validating Data. EXITING.\nERROR:{}".format( e ))
        sys.exit(1)


def strip(string):
    '''
    '''
    return string.strip('`').strip('"').strip('[').strip(']')


def encryption_count_check(csv_path, column, src_db_host, src_db_port, dest_db_host, dest_db_port, root_user, root_pass):
    
    with open(csv_path, 'r') as f:
        data = csv.reader(f)

        for line in data:
            db = line[0].split('.')[0]
            table = line[0].split('.')[-1]
            schema = line[0].split('.')[1]

            original_data = int(line[1])
            column = column
            start_db = 'MSSQL_LargeData'

            if src_db_port == '1521':
                query_clear_not_null = 'select count(*) from {}.{} where {} is not null'.format(schema, table, column)
                query_clear_null = 'select count(*) from {}.{} where {} is null'.format(schema, table, column)
            elif src_db_port == '1433':
                query_clear_not_null = 'select count(*) from {}.{}.{} where {} is not null;'.format(start_db, strip(db), strip(table), strip(column))
                query_clear_null = 'select count(*) from {}.{}.{} where {} is null;'.format(start_db, strip(db), strip(table), strip(column))
            elif src_db_port == '3306':
                query_clear_not_null = 'select count(*) from {}.{} where {} is not null'.format(db, table, column)
                query_clear_null = 'select count(*) from {}.{} where {} is null'.format(db, table, column)
            elif src_db_port == '5432':
                query_clear_not_null = 'select count(*) from "{}".{}.{} where {}::text is not null'.format(db, schema, table, column)
                query_clear_null = 'select count(*) from "{}".{}.{} where {}::text is null'.format(db, schema, table, column)

            original_not_null = run_query(src_db_host, src_db_port, root_user, root_pass, query_clear_not_null, db)
            ONN = original_not_null[0][0]
            original_null = run_query(src_db_host, src_db_port, root_user, root_pass, query_clear_null, db)
            ON = original_null[0][0]

            if dest_db_port == '3306':
                query_enc_not_null = 'select count(*) from {}.{} where {} is not null;'.format(strip(db), strip(table), strip(column))
                query_enc_null = 'select count(*) from {}.{} where {} is null;'.format(strip(db), strip(table), strip(column))
            elif dest_db_port == '5432':
                query_enc_not_null = 'select count(*) from "{}".{}.{} where {}::text is not null'.format(db, schema, table, column)
                query_enc_null = 'select count(*) from "{}".{}.{} where {}::text is null'.format(db, schema, table, column)
            
            enc_not_null = run_query(dest_db_host, dest_db_port, root_user, root_pass, query_enc_not_null, strip(db))
            ENN = enc_not_null[0][0]
            enc_null = run_query(dest_db_host, dest_db_port, root_user, root_pass, query_enc_null, strip(db))
            EN = enc_null[0][0]

            ENC_COUNT_CHECK = True
            if ONN == ENN and ON == EN:
                if ONN + ON == original_data:
                    pass
                else:
                    ENC_COUNT_CHECK = False
            else:
                ENC_COUNT_CHECK = False
            
            if ENC_COUNT_CHECK:
                status = 'PASS'
            else:
                status = 'FAIL'
            
            if dest_db_port == '5432':
                result = ('{}, {}, {}, {}, {}, {}, {}, {}, {} : {}'.format(db, strip(schema), strip(table), column, original_data, ONN, ON, ENN, EN, status))
            elif dest_db_port == '3306':
                result = ('{}, {}, {}, {}, {}, {}, {}, {} : {}'.format(db, strip(table), column, original_data, ONN, ON, ENN, EN, status))
    return result


def encryption_check(src_db_host, src_db_port, dest_db_host, dest_db_port, root_user, root_pass, BPS_path, BES_path, csv_path):
    '''
    '''
    print('\nRunning encryption check:\n')
    table_columns_dict, _ = parse_BPS(BPS_path)

    for table, columns in table_columns_dict.items():
        database = strip(table.split('.')[0])
        
        for column in columns:
            query = None
            if dest_db_port == '3306':
                query = 'select HEX({}) from {} where {} is not null;'.format(column, table, column)
            elif dest_db_port == '5432':
                query = 'select {}::text from {} where {}::text is not null;'.format(column, table, column)

            query_output = run_query(dest_db_host, dest_db_port, root_user, root_pass, query, database)
            
            for record in query_output:
                is_col_fail = False

                if dest_db_port == '3306':
                    enc_string = record[0].decode('utf-8')
                elif dest_db_port == '5432':
                    enc_string = record[0].lstrip('\\x')

                if not enc_string.startswith(('BEBEBEBC', 'bebebebc')):
                    is_col_fail = True

            if is_col_fail:
                print('ENCRYPTION FAIL: Column: {}'.format(column))
            else:
                print('ENCRYPTION PASS: Column: {}'.format(column))

            print(encryption_count_check(csv_path, column, src_db_host, src_db_port, dest_db_host, dest_db_port, root_user, root_pass))


def decryption_check(src_db_host, src_db_port, bs_host, bs_port, dest_db_port, root_user, root_pass, BPS_path, BES_path):
    print('\nRunning decryption check:\n')

    table_columns_dict, column_type_dict = parse_BPS(BPS_path)

    for table_name, columns in table_columns_dict.items():
        db = strip(table_name.split('.')[0])
        table = strip(table_name.split('.')[-1])
        schema = strip(table_name.split('.')[1])

        for column in columns:
            output_file_dec = None
            output_file_clear = None
            query_clear = None
            query_dec = None

            start_db = 'MSSQL_LargeData'

            if src_db_port == '1521':
                src_db = 'oracle'
            elif src_db_port == '5432':
                src_db = 'postgres'
            elif src_db_port == '1433':
                src_db = 'sql-server'
            elif src_db_port == '3306':
                src_db = 'mysql'
            

            #File creation for clear data from source DB
            if src_db_port == '3306':
                output_file_clear = '{}/{}_{}_{}_clear.txt'.format(decryption_logs, db, table, strip(column))
            elif src_db_port == '5432':
                output_file_clear = '{}/{}_{}_{}_{}_clear.txt'.format(decryption_logs, db, schema, table, strip(column))
            elif src_db_port == '1521':
                output_file_clear = '{}/{}_{}_{}_{}_clear.txt'.format(decryption_logs, schema, table, strip(column), src_db)
            elif src_db_port == '1433':
                output_file_clear = '{}/{}_{}_{}_clear.txt'.format(decryption_logs, db, table, strip(column))
            
            #File creation for decrypted data from destination DB
            if dest_db_port == '3306':
                output_file_dec = '{}/{}_{}_{}_dec.txt'.format(decryption_logs, db, table, strip(column))
            else:
                output_file_dec = '{}/{}_{}_{}_{}_{}_dec.txt'.format(decryption_logs, db, schema, table, strip(column), get_db_type(int(dest_db_port)))

            #Query for clear data from source DB
            if src_db_port == '3306':
                query_clear = 'select {} from {}.{}'.format(column, db, table)
            elif src_db_port == '5432':
                query_clear = 'select {} from "{}"."{}";'.format(column, schema, table)
            elif src_db_port == '1521':
                query_clear = 'select {} from "{}"."{}"'.format(strip(column), schema, table)
            elif src_db_port == '1433':
                query_clear = 'SELECT {} from {}.{}.{};'.format(strip(column), start_db, db, table)

            #Query for decrypted data from destination DB
            if dest_db_port == '3306':
                query_dec = 'select {} from {}.{};'.format(column, db, table)
            else:
                query_dec = 'select {} from {};'.format(column, table_name)
            
            if strip(column) != 'col_datetimeoffset':
                query_output = run_query(src_db_host, src_db_port, root_user, root_pass, query_clear, src_db)
            else:
                query_output = run_query_datetimeoffset(src_db_host, src_db_port, root_user, root_pass, query_clear, src_db)

            with open(output_file_clear, 'w') as f:
                for item in query_output:
                    if type(item[0]) is memoryview:
                        item = binascii.hexlify(item[0])
                        f.write("%s\n" % str(item[0]).rstrip())
                    else:
                        f.write("%s\n" % str(item[0]).rstrip())

            query_output = run_query(bs_host, bs_port, root_user, root_pass, query_dec, db)

            with open(output_file_dec, 'w') as f:
                for item in query_output:
                    if column_type_dict[column].startswith(('TEXT','VARCHAR')):
                        if type(item[0]) is memoryview:
                            item = binascii.hexlify(item[0])
                            if item[0] is None:
                                f.write("%s\n" % str(item[0]))
                            else:
                                f.write("%s\n" % str(bytes(item[0]).decode('utf-8')))
                        else:
                            if item[0] is None:
                                f.write("%s\n" % str(item[0]))
                            else:
                                f.write("%s\n" % str(bytes(item[0]).decode('utf-8')))

                    elif column_type_dict[column].startswith(('BOOL')):
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
                                f.write("%s\n" % str(None))

                    else:
                        if type(item[0]) is memoryview:
                            item = binascii.hexlify(item[0])
                            f.write("%s\n" % str(item[0]))
                        else:
                            f.write("%s\n" % str(item[0]))
        
            is_clear_file_empty = os.stat(output_file_clear).st_size==0
            is_dec_file_empty = os.stat(output_file_dec).st_size==0

            if is_clear_file_empty or is_dec_file_empty:
                print('Decryption Validation failed for column {}.{}, empty file(s)'.format(table_name, column))
            else:
                output_file_clear_sorted = output_file_clear + '.sorted'
                output_file_dec_sorted = output_file_dec + '.sorted'

                os.system('sort {} > {}'.format(output_file_clear, output_file_clear_sorted))
                os.system('sort {} > {}'.format(output_file_dec, output_file_dec_sorted))

                if filecmp.cmp(output_file_clear_sorted, output_file_dec_sorted):
                    print('Decryption Validation passed for column {}.{}'.format(table, column))
                else:
                    print('Decryption Validation failed for column {}.{}'.format(table, column))
            #sys.exit(1)


def get_db_type(db_port):
    '''
    Return name of database.
    '''
    db_type = None

    if db_port == 1521:
        db_type = 'oracle'
    elif db_port == 5432:
        db_type = 'postgres'
    elif db_port == 1433:
        db_type = 'sql-server'
    elif db_port == 3306:
        db_type = 'mysql'

    return db_type


log_dir, decryption_logs = create_log_dirs()

encryption_check(src_db_host, src_db_port, dest_db_host, dest_db_port, root_user, root_pass, BPS_path, BES_path, csv_path)

#decryption_check(src_db_host, src_db_port, bs_host, bs_port, dest_db_port, root_user, root_pass, BPS_path, BES_path)