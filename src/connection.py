import psycopg2
from config import config

def connect():
    """
    connect to a postgres server
    :return:
    """
    conn = None
    try:
        params = config()
        print("Connecting to the posgresSQL database...")
        conn = psycopg2.connect(**params)

        cur = conn.cursor()
        print('Connection to the database successfull !')
        #cur.execute("SELECT version()")
        #db_version = cur.fetchone()
        #print(db_version)
        #cur.close()
        return conn
    except (Exception,psycopg2.DatabaseError) as error:
        print(error)



if __name__=='__main__':
    connect()