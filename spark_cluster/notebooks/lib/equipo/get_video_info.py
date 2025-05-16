# pip install google-api-python-client
from googleapiclient.discovery import build

api_key = 'mi_api, jaja'
youtube = build('youtube', 'v3', developerKey=api_key)

def get_video_info(video_id):
    request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=video_id
    )
    response = request.execute()

    if not response['items']:
        return None

    item = response['items'][0]
    info = {
        'title': item['snippet']['title'],
        #'description': item['snippet']['description'],
        'tags': item['snippet'].get('tags', []),
        'publishedAt': item['snippet']['publishedAt'],
        'channelId': item['snippet']['channelId'],
        'channelTitle': item['snippet']['channelTitle'],
        'viewCount': item['statistics'].get('viewCount'),
        'likeCount': item['statistics'].get('likeCount'),
        'commentCount': item['statistics'].get('commentCount'),
        'duration': item['contentDetails']['duration'],
        'categoryId': item['snippet']['categoryId']
    }
    return info

if __name__ == "__main__":
    video_id = 'dQw4w9WgXcQ'
    info = get_video_info(video_id)
    print(info)
