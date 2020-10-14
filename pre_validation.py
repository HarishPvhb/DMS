import mysql.connector
import psycopg2
from collections import defaultdict
import sys, os
import filecmp
import binascii
import time

# parameters to be passed to this script.

bs_host = sys.argv[1]
db_host = sys.argv[2]
bs_port = sys.argv[3]
db_port = sys.argv[4]
root_user = sys.argv[5]
root_pass = sys.argv[6]

BPS_path = sys.argv[7]
BES_path = sys.argv[8]

res_dir = sys.argv[9]



def create_log_dirs():
    '''
    Creating log dir to store temp and log files.
    '''
    log_dir = '/tmp/workload_logs'
    decryption_logs =  log_dir + '/decryption_logs'

    os.system('mkdir -p {}'.format(log_dir))
    os.system('mkdir -p {}'.format(decryption_logs))

    return log_dir, decryption_logs


def parse_BPS(file_path):
    '''
    Returns db_name.table_name.col_name.
    '''
    table_columns_dict = defaultdict(set)

    with open(file_path, 'r', encoding='utf-8', errors='replace') as fp:
        lines = fp.readlines()

    for line in lines:
        table = ".".join(line.split(' ')[0].split('.')[:-1])
        column = line.split(' ')[0].split('.')[-1]

        table_columns_dict[table].add(column)

    return table_columns_dict


def get_connection(host, user, password, port, database=None):
    '''
    Returns connection object as per the DB used.
    '''
    connection = None
    if db_type == 'mysql':
        connection = mysql.connector.connect(host=host,
                                             user=user,
                                             passwd=password,
                                             use_unicode=False,
                                             port=port)
    elif db_type == 'postgres':
        connection = psycopg2.connect(database=database,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port)
    elif db_type == 'mssql':
        connection = None

    return connection


def run_query(host, port, root_user, root_pass, query, database=None):
    '''
    '''
    if db_port == '1433':
        output_file="/tmp/temp_out"
        cmd_list = [
                    "sqlcmd",
                    "-m 1",
                    "-h -1",
                    "-S", "tcp:{},{}".format(host, port),
                    "-U", root_user,
                    "-P", root_pass,
                    "-Q",'"{}"'.format(query),
                    ">", output_file
                    ]
        os.system(" ".join(cmd_list))

        with open(output_file, 'r', encoding='utf-8', errors='replace') as fp:
            lines = fp.readlines()
        
        return lines[:-2]

    else:
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


def strip(string):
    '''
    '''
    return string.strip('`').strip('"').strip('[').strip(']')


def encryption_check(db_host, db_port, root_user, root_pass, BPS_path, BES_path):
    '''
    '''
    print('\nRunning encryption check:\n')
    table_columns_dict = parse_BPS(BPS_path)

    for table, columns in table_columns_dict.items():
        database = strip(table.split('.')[0])
        
        for column in columns:
            query = None
            if db_port == '3306':
                query = 'select HEX({}) from {} where {} is not null;'.format(column, table, column)
            elif db_port == '5432':
                query = "select {}::text from {} where {}::text is not null;".format(column, table, column)
            elif db_port == '1433':
                query="SELECT CONVERT(varchar(max),{},1) from {} where CONVERT(varchar(max),{},1) is not null;".format(column, table, column)

            query_output = run_query(db_host, db_port, root_user, root_pass, query, database)

            for record in query_output:
                is_col_fail = False

                if db_port == '3306':
                    enc_string = record[0].decode('utf-8')
                elif db_port == '1433':
                    enc_string = record.lstrip('\\0x')
                else:
                    enc_string = record[0].lstrip('\\x')

                if not enc_string.startswith(('BEBEBEBC', 'bebebebc')):
                    is_col_fail = True

            if is_col_fail:
                print('ENCRYPTION FAIL: Column: {}'.format(column))
            else:
                print('ENCRYPTION PASS: Column: {}'.format(column))


def decryption_check(db_host, db_port, root_user, root_pass, BPS_path, BES_path):
    '''
    '''
    print('\nRunning decryption check:\n')

    table_columns_dict = parse_BPS(BPS_path)

    for table_name, columns in table_columns_dict.items():
        db = strip(table_name.split('.')[0])
        table = strip(table_name.split('.')[-1])
        schema = strip(table_name.split('.')[1])

        for column in columns:
            output_file_dec = None
            output_file_clear = None
            query_clear = None
            query_dec = None

            if db_port == '3306':
                output_file_dec = '{}/{}_{}_{}_dec.txt'.format(decryption_logs, db, table, strip(column))
                output_file_clear = '{}/{}_{}_{}_clear.txt'.format(decryption_logs, db, table, strip(column))
            else:
                output_file_dec = '{}/{}_{}_{}_{}_dec.txt'.format(decryption_logs, db, schema, table, strip(column))
                output_file_clear = '{}/{}_{}_{}_{}_clear.txt'.format(decryption_logs, db, schema, table, strip(column))


            if db_port == '3306':
                query_dec = 'select {} from {}.{}'.format(column, db, table)
                query_clear = 'select {} from {}_clear.{}'.format(column, db, table)
            elif db_port == '5432':
                query_dec = 'select {} from {};'.format(column, table_name)
                query_clear = 'select {} from "{}"."{}";'.format(column, schema, table)
            elif db_port == '1433':
                query_dec = "SELECT{} from {};".format(column, table)
                query_clear = "SELECT{} from {};".format(column, table)

            query_output = run_query(bs_host, bs_port, root_user, root_pass, query_dec, db)

            with open(output_file_dec, 'w') as f:
                for item in query_output:
                    if type(item[0]) is memoryview:
                        item = binascii.hexlify(item[0])
                        f.write("%s\n" % str(item))
                    else:
                        f.write("%s\n" % str(item))

            clear_db = db + '_clear'
            query_output = run_query(db_host, db_port, root_user, root_pass, query_clear, clear_db)

            with open(output_file_clear, 'w') as f:
                for item in query_output:
                    if type(item[0]) is memoryview:
                        item = binascii.hexlify(item[0])
                        f.write("%s\n" % str(item))
                    else:
                        f.write("%s\n" % str(item))

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
                    print('Decryption Validation passed for column {}.{}'.format(table_name, column))
                else:
                    print('Decryption Validation failed for column {}.{}'.format(table_name, column))


def get_db_type(db_port):
    '''
    Return name of database.
    '''
    db_type = None

    if db_port == '3306':
        db_type = 'mysql'
    elif db_port == '5432':
        db_type = 'postgres'
    elif db_port == '1433':
        db_type = 'mssql'
    
    return db_type



db_type = get_db_type(db_port)

log_dir, decryption_logs = create_log_dirs()

encryption_check(db_host, db_port, root_user, root_pass, BPS_path, BES_path)

decryption_check(db_host, db_port, root_user, root_pass, BPS_path, BES_path)
