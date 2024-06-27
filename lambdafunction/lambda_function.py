import json
import boto3
import pymysql
import logging
from collections import defaultdict
import requests
import os

# Constants
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
POSTS_DB_NAME = os.environ['POST_DB_NAME']
MEDIA_DB_NAME = os.environ['MEDIA_DB_NAME']
COMMENT_DB_NAME = os.environ['COMMENT_DB_NAME']


DOMAIN_ENDPOINT = os.environ['DOMAIN_ENDPOINT']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        
        if http_method == 'GET':
            logger.info("GET Method")
            query_parameters = event.get('queryStringParameters', {})
            
            logger.info("Query Params")
            logger.info(query_parameters)
            
            query = query_parameters['query']
            
            return search(query, DOMAIN_ENDPOINT)
            
        else:
            return {
                'statusCode': 405,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'error': 'Method Not Allowed'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': str(e)})
        }
    
def search(query, domain_endpoint):
    url = f"https://{domain_endpoint}/_search"
    headers = {"Content-Type": "application/json"}
    payload = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["username", "content"],
                "fuzziness": "AUTO"
            }
        }
    }

    response = requests.get(url, headers=headers, json=payload)
    logger.info("search response")
    search_result = response.json()
    logger.info(search_result)
    
    processed_results = process_search_results(search_result)
    
    return {
        'statusCode': 200,
        'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
        'body': json.dumps({'result':processed_results})
        
    }
    
def get_post_ids(posts):
    post_ids = []
    
    for post in posts:
        if post['_index'] == "posts":
            post_ids.append( post['_source']['id'] ) 
        
    return post_ids
    
def process_search_results(results):
    logger.info("Setting Variables")
    processed_results = defaultdict(list)
    users = []
    posts = []
    tmp_posts = []
    
    try:
        logger.info("Separating Post IDs")
        post_ids = get_post_ids(results['hits']['hits'])
        
        if post_ids:
            logger.info("Getting comments for the posts")
            comments = get_comments_by_post_id(post_ids)
            
            logger.info("Getting Media Metadata for the posts")
            media_metadata = get_media_metadata_by_post_ids(post_ids)
            
            logger.info("Separating Users and Posts from results")
            for result in results['hits']['hits']:
                if result['_index'] == "users":
                    users.append( result['_source'] )
                elif result['_index'] == "posts":
                    tmp_posts.append( result['_source'] )
            
            posts = combine_posts_with_media(tmp_posts, comments, media_metadata)
        
        processed_results['users'] = users
        processed_results['posts'] = posts
        
        return processed_results
    except Exception as e:
        logger.info(str(e))
        raise e
    
    
def get_posts_by_username(username):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=POSTS_DB_NAME)
    
    logger.info("Get Posts by username")
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, user_id, username, content FROM posts WHERE LOWER(username) LIKE LOWER( %s )"
            cursor.execute(sql, ('%' + username + '%',))
            results = cursor.fetchall()
            post_list = []
            for post in results:
                post_dict = {
                    "id":post[0],
                    "user_id":post[1],
                    "username":post[2],
                    "content":post[3]
                }
                post_list.append(post_dict)
            
        logger.info(post_list)
        return post_list
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()



def get_media_metadata_by_post_ids(post_ids):
    if not post_ids:
        return []
    
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=MEDIA_DB_NAME)
    logger.info("Get metadata for media in post")
    post_id_tuple = tuple(post_ids)
    try:
        with connection.cursor() as cursor:
            sql = "select user_id, post_id, s3_key, url, size, type  from media_metadata where post_id in %s"
            cursor.execute(sql, (post_id_tuple,))
            results = cursor.fetchall()
            logger.info("media metadata")
            logger.info(results)
            media_list = []
            for media in results:
                media_dict = {
                    "user_id": media[0],
                    "post_id": media[1],
                    "s3_key": media[2],
                    "url" : media[3],
                    "size": media[4],
                    "type": media[5]
                }
                media_list.append(media_dict)
            logger.info("media list")
            logger.info(media_list)
        return media_list
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()
        
def get_comments_by_post_id(post_ids):
    if not post_ids:
        return []
    
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=COMMENT_DB_NAME)
    logger.info("Get comments for posts")
    post_id_tuple = tuple(post_ids)
    logger.info("post id's")
    logger.info(post_id_tuple)
    try:
        with connection.cursor() as cursor:
            sql = "select id, user_id, post_id, content, created_at, username from comments where post_id in %s"
            cursor.execute(sql, (post_id_tuple,))
            results = cursor.fetchall()
            logger.info("post comments")
            logger.info(results)
            comment_dict = defaultdict(list)
            
            for comment in results:
                
                comment_object = {
                    "id":comment[0],
                    "post_id":comment[2],
                    "user_id":comment[1],
                    "username":comment[5],
                    "content":comment[3],
                    "created_at":comment[4].strftime('%Y-%m-%d %H:%M:%S')
                }
                
                comment_dict[comment_object['post_id']].append(comment_object)
                
            logger.info("comment dictionary")
            logger.info(comment_dict)
        return comment_dict
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()
        
def combine_posts_with_media(posts, comments, media_metadata):
    logger.info("Combining media to the post")
    logger.info(media_metadata)
    media_dict = {}
    for media in media_metadata:
        post_id = media['post_id']
        if post_id not in media_dict:
            media_dict[post_id] = []
        media_dict[post_id].append(media)
    
    for post in posts:
        post['media_metadata'] = media_dict.get(post['id'], [])
        post['comments'] = comments.get(post['id'],[])
    
    return posts


