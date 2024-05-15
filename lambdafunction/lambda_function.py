import json
import pymysql

def lambda_handler(event, context):
    print("trying")
    try:
        host = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
        print("connecting to database", host)
        
        connection = pymysql.connect(
            host=host,
            user='admin',
            password='FrostGaming1!',
            database='comment_db'
        )
        
        print("Connection Successful executing script")
    
        
        cursor = connection.cursor()
        
        result = None
        
        with open('comment_table.sql', 'r') as file:
            script = file.read()
            cursor.execute(script)
            result = cursor.fetchall()
            print(result)
        
        connection.commit()
        
        print("script executed")
        
        cursor.close()
        connection.close()
    
    except Exception as err:
        print(err)
    
        
    return {
        'statusCode': 200,
        'body': json.dumps('db test')
    }
