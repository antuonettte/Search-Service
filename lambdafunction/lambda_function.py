import json
import boto3
import pymysql

# Constants
POSTS_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
POSTS_DB_USER = 'admin'
POSTS_DB_PASSWORD = 'FrostGaming1!'
POSTS_DB_NAME = 'posts_db'

MEDIA_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
MEDIA_DB_USER = 'admin'
MEDIA_DB_PASSWORD = 'FrostGaming1!'
MEDIA_DB_NAME = 'media_metadata_db'

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        
        if http_method == 'GET':
            query_parameters = event.get('queryStringParameters', {})
            
            if query_parameters and 'username' in query_parameters:
                return search_posts_by_username(query_parameters['username'])
            elif query_parameters and 'content' in query_parameters:
                return search_posts_by_content(query_parameters['content'])
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required query parameters'})
                }
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def search_posts_by_username(username):
    try:
        posts = get_posts_by_username(username)
        post_ids = [post['post_id'] for post in posts]
        media_metadata = get_media_metadata_by_post_ids(post_ids)
        
        combined_results = combine_posts_with_media(posts, media_metadata)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'results': combined_results})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def search_posts_by_content(content):
    try:
        posts = get_posts_by_content(content)
        post_ids = [post['post_id'] for post in posts]
        media_metadata = get_media_metadata_by_post_ids(post_ids)
        
        combined_results = combine_posts_with_media(posts, media_metadata)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'results': combined_results})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_posts_by_username(username):
    connection = pymysql.connect(host=POSTS_DB_HOST,
                                 user=POSTS_DB_USER,
                                 password=POSTS_DB_PASSWORD,
                                 database=POSTS_DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT post_id, user_id, username, content FROM posts WHERE username LIKE %s"
            cursor.execute(sql, ('%' + username + '%',))
            results = cursor.fetchall()
        return results
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()

def get_posts_by_content(content):
    connection = pymysql.connect(host=POSTS_DB_HOST,
                                 user=POSTS_DB_USER,
                                 password=POSTS_DB_PASSWORD,
                                 database=POSTS_DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT post_id, user_id, username, content FROM posts WHERE content LIKE %s"
            cursor.execute(sql, ('%' + content + '%',))
            results = cursor.fetchall()
        return results
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()

def get_media_metadata_by_post_ids(post_ids):
    if not post_ids:
        return []
    
    connection = pymysql.connect(host=MEDIA_DB_HOST,
                                 user=MEDIA_DB_USER,
                                 password=MEDIA_DB_PASSWORD,
                                 database=MEDIA_DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT post_id, s3_key, url, size, type FROM media_metadata WHERE post_id IN %s"
            cursor.execute(sql, (post_ids,))
            results = cursor.fetchall()
        return results
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()

def combine_posts_with_media(posts, media_metadata):
    media_dict = {}
    for media in media_metadata:
        post_id = media['post_id']
        if post_id not in media_dict:
            media_dict[post_id] = []
        media_dict[post_id].append(media)
    
    for post in posts:
        post['media_metadata'] = media_dict.get(post['post_id'], [])
    
    return posts

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        
        if http_method == 'GET':
            query_parameters = event.get('queryStringParameters', {})
            
            if 'username' in query_parameters:
                return search_posts_by_username(query_parameters['username'])
            elif 'content' in query_parameters:
                return search_posts_by_content(query_parameters['content'])
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required query parameters'})
                }
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def search_posts_by_username(username):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT p.post_id, p.user_id, p.username, p.content, m.s3_key, m.url, m.size, m.type
            FROM posts p
            LEFT JOIN media_metadata m ON p.post_id = m.post_id
            WHERE p.username LIKE %s
            """
            cursor.execute(sql, ('%' + username + '%',))
            results = cursor.fetchall()

        connection.commit()
        
        return {
            'statusCode': 200,
            'body': json.dumps({'results': results})
        }
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()

def search_posts_by_content(content):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT p.post_id, p.user_id, p.username, p.content, m.s3_key, m.url, m.size, m.type
            FROM posts p
            LEFT JOIN media_metadata m ON p.post_id = m.post_id
            WHERE p.content LIKE %s
            """
            cursor.execute(sql, ('%' + content + '%',))
            results = cursor.fetchall()

        connection.commit()
        
        return {
            'statusCode': 200,
            'body': json.dumps({'results': results})
        }
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()
