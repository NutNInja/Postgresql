import csv
import psycopg2

# Just Execute Sql , No result returns
def ExecuteSql(sql):
    try:
        conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()
        print('Connected to %s' % host)
    except Exception as e:
        print(e)
        return False
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
    return True

# Execute Select Sql and get the result
def GetSqlResult(sql):
    try:
        conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()
        print('Connected to %s' % host)
    except Exception as e:
        print(e)
        return False
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except Exception as e:
        print(e)
        return False
    finally:
        cursor.close()
        conn.close()
    return result

if __name__ == "__main__":
    # Settings
    database = ' '
    username = ' '
    password = ' '
    host = ' '
    port = '5434'

    # Test sql
    sql1 = '''drop table if exists test1; \
             select * into test1 from shanghai_4807 where id = 2;'''
    sql2 = '''select * from shanghai_4807 where id in (1,2,3);'''

    ExecuteSql(sql1)
    result = GetSqlResult(sql2)
    csvfile = open('G:/PyCharmProjects/result.csv','w')
    writer = csv.writer(csvfile)
    writer.writerows(result)
    csvfile.close()