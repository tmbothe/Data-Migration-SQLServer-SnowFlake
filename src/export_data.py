from connection import connect
from s3_utils import *
import pandas as pd
import os


def check_folder(bucket_name, folder_name):
    resp = s3_client('s3').list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_name,
        MaxKeys=1
    )
    return resp


def run_query(bucket_name, conn, tables):
    cursor = conn.cursor()

    for table in tables:
        query = f'SELECT * FROM public.{table}'
        print('Table loaded in a dataframe')
        i = 1

        chunks = pd.read_sql_query(query, conn, chunksize=100)

        for chunk in chunks:
            with open("out.csv", "w") as fh:
                chunk.to_csv(fh, index=False)
            print(f'Processing chunk {i}.')
            file_name = f"{table}_{i}.csv"
            file_path = os.path.join(os.getcwd(), 'out.csv')
            print(f'file path is {file_path}')
            #upload_large_file(bucket_name, file_path, file_name)
            upload_large_file(bucket_name=bucket_name,
                              file_path=file_path, file_name=f'{table}/{file_name}')
            i += 1

    cursor.close()


if __name__ == '__main__':
    tables = ['accounts', 'orders', 'region']
    bucket_name = 'thim-snow1'
    conn = connect()
    print(run_query(bucket_name, conn, tables))

    '''

    chunks = pd.read_sql_query(
        "SELECT * FROM public.accounts", conn, chunksize=10000)

    i = 1
    for chunk in chunks:
        file_name = f'account_{i}'
        chunk.to_csv(file_name, index=False)
        print(f"Loop {i} ")
        i += 1
    '''
