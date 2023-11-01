# importing Library using in this Project...
import streamlit as st
import pandas as pd
import urllib
import ssl
import mysql.connector as sql 
import time
import plotly.express as px 
from googleapiclient.discovery import build
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from googleapiclient.errors import HttpError

# streamlit design -1
st.set_page_config(page_title="YouTube WereHouse",layout="wide")
left_column, right_column = st.columns(2)

# config for youtube API
api_key = 'AIzaSyCaFUxKnpupwgjCIRspJyqf9v5brozYHtg'

# connection  mongoDb
username = "dinesh" 
password = "Din@9600" 

encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)

ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.tdjuevi.mongodb.net/YouTube?retryWrites=true&w=majority"

client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")

# Access the YouTube database and collection..
db = client['YouTube_1']
collection = db['testing_data']
# connection to mysql...
db_connection = sql.connect(
    host="localhost",
    user="root",
    password="Din@9600",
    database="youtubedata"
)
mycursor = db_connection.cursor(buffered=True)
cursor = db_connection.cursor()


class YouTubeDataRetriever:
    def __init__(self, api_key):
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def get_video_ids(self, playlist_id):
        request = self.youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50)
        response = request.execute()

        video_ids = []

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = self.youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()

                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')

        return video_ids

    def get_video_details(self, video_ids):
        videos = []

        for i in range(0, len(video_ids), 50):
            request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids[i:i + 50]))

            video_response = request.execute()

            videos.extend(video_response['items'])
        return videos
    
    def playlist(self, channel_id):
            request = self.youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=channel_id,
                maxResults=50)
            response = request.execute()

            playlist = []

            for i in range(0, len(response['items'])):
                data = {'playlist_id': response['items'][i]['id'],
                        'playlist_name': response['items'][i]['snippet']['title'],
                        'channel_id': channel_id}

                playlist.append(data)

            next_page_token = response.get('nextPageToken')

            # manually set umbrella = True for breaking while condition
            umbrella = True

            while umbrella:
                if next_page_token is None:
                    umbrella = False

                else:
                    request = self.youtube.playlists().list(
                        part="snippet,contentDetails,status",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token)
                    response = request.execute()

                    for i in range(0, len(response['items'])):
                        data = {'playlist_id': response['items'][i]['id'],
                                'playlist_name': response['items'][i]['snippet']['title'],
                                'channel_id': channel_id
                                }

                        playlist.append(data)

                    next_page_token = response.get('nextPageToken')

            return playlist
            
    def get_video_comments(self, videoid):
        comments = []

        try:
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=videoid,
                maxResults=100
            )

            while request:
                response = request.execute()

                for comment in response['items']:
                    data = {
                        'Video_Id': videoid,
                        'Comment_Id': comment['snippet']['topLevelComment']['id'],
                        'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_PublishedAt': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comments.append(data)

                if 'nextPageToken' in response:
                    request = self.youtube.commentThreads().list(
                        part="snippet",
                        textFormat="plainText",
                        videoId=videoid,
                        maxResults=100,
                        pageToken=response.get('nextPageToken')
                    )
                else:
                    break
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                data = {
                    'Video_Id': videoid,
                    'Comment_Id': f'comments_disabled_{videoid}',
                    'Comment_Text': 'comments_disabled',
                    'Comment_Author': 'comments_disabled',
                    'Comment_PublishedAt': 'comments_disabled'
                }
                comments.append(data)
                print(f"Comments are disabled for video: {videoid}")
            else:
                print(f"An error occurred while retrieving comments for video: {videoid}")
                print(f"Error details: {e}")

        return comments

    def parse_duration(self, duration):
        duration_str = ""
        hours = 0
        minutes = 0
        seconds = 0

        # Remove 'PT' prefix from duration
        duration = duration[2:]

        # Check if hours, minutes, and/or seconds are present in the duration string
        if "H" in duration:
            hours_index = duration.index("H")
            hours = int(duration[:hours_index])
            duration = duration[hours_index + 1:]
        if "M" in duration:
            minutes_index = duration.index("M")
            minutes = int(duration[:minutes_index])
            duration = duration[minutes_index + 1:]
        if "S" in duration:
            seconds_index = duration.index("S")
            seconds = int(duration[:seconds_index])

        # Format the duration string
        if hours >= 0:
            duration_str += f"{hours}h "
        if minutes >= 0:
            duration_str += f"{minutes}m "
        if seconds >= 0:
            duration_str += f"{seconds}s"

        return duration_str.strip()

    def retrieve_channel_data(self, channel_id):
        request = self.youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        )
        response = request.execute()

        if 'items' in response:
            channel_data = response['items'][0]
            snippet = channel_data['snippet']
            statistics = channel_data['statistics']
            content_details = channel_data.get('contentDetails', {})
            related_playlists = content_details.get('relatedPlaylists', {})
            
            # playlist_data = self.playlist(channel_id) # newadded

            # Extract relevant data
            data = {
                'Channel_Name': {
                    'Channel_Name': snippet.get('title', ''),
                    'Channel_Id': channel_id,
                    'Subscription_Count': int(statistics.get('subscriberCount', 0)),
                    'Channel_Views': int(statistics.get('viewCount', 0)),
                    'Channel_Description': snippet.get('description', ''),
                    'Playlist_Id': related_playlists.get('uploads', '')
                }
                
            }
            # Retrieve video data
            video_ids = self.get_video_ids(data['Channel_Name']['Playlist_Id'])
            videos = self.get_video_details(video_ids)
            # data["vidoes"] = videos

            for video in videos:
                video_id = video['id']
                video_data = {
                    'Video_Id': video_id,
                    'Video_Name': video['snippet'].get('title', ''),
                    'Video_Description': video['snippet'].get('description', ''),
                    'Tags': video['snippet'].get('tags', []),
                    'PublishedAt': pd.to_datetime(video['snippet'].get('publishedAt', '')),
                    'View_Count': int(video['statistics'].get('viewCount', 0)),
                    'Like_Count': int(video['statistics'].get('likeCount', 0)),
                    'Dislike_Count': int(video['statistics'].get('dislikeCount', 0)),
                    'Favorite_Count': int(video['statistics'].get('favoriteCount', 0)),
                    'Comment_Count': int(video['statistics'].get('commentCount', 0)),
                    'Duration': self.parse_duration(video['contentDetails'].get('duration', '')),
                    'Thumbnail': video['snippet'].get('thumbnails', {}).get('default', {}).get('url', ''),
                    'Caption_Status': video['snippet'].get('localized', {}).get('localized', 'Not Available'),
                    'Comments': self.get_video_comments(video_id)
                }
                data[video_id] = video_data
                
            # data["playlist"]=playlist_data # new add

            return data

class DataMigration():
    def durationtoint(time_str):
        hours, minutes, seconds = time_str.split('h ')[0], time_str.split('h ')[1].split('m ')[0], \
            time_str.split('h ')[1].split('m ')[1][:-1]

        total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        return (total_seconds)
    def migratetoSql(id):
        migration_data = collection.find_one({'Channel_Name.Channel_Id': id})
        if migration_data:
                query_channel = """
                INSERT INTO Channel (
                    channel_id,
                    channel_name,
                    Channel_subscription,
                    channel_views,
                    channel_description
                    ) VALUES (%s, %s, %s, %s, %s)
                    """
                values_channel = (
                    migration_data['Channel_Name']['Channel_Id'],
                    migration_data['Channel_Name']['Channel_Name'],
                    migration_data['Channel_Name']['Subscription_Count'],
                    migration_data['Channel_Name']['Channel_Views'],
                    migration_data['Channel_Name']['Channel_Description']
                )
                # cursor.execute(query_channel, values_channel)
                
                # Assuming 'migration_data' is a MongoDB collection
                # Assuming migration_data is a dictionary
                # for i in migration_data['playlist']:
                #     playlist_id = i.get("playlist_id")
                #     channel_id = i.get("channel_id")
                #     playlist_name = i.get("playlist_name")
                #     channel_name = migration_data.get("channel_name") 
                    
                #     query_playlist = """
                #     INSERT INTO playlist (
                #         playlist_id,
                #         channel_id,
                #         playlist_name,
                #         channel_name
                #         ) VALUES (%s, %s, %s,%s)
                #         """
                #     values_playlist = (
                #         # migration_data['Channel_Name']['Playlist_Id'],
                #         # migration_data['Channel_Name']['Channel_Id'],
                #         playlist_id,
                #         channel_id,
                #         playlist_name,
                #         channel_name
                #         # migration_data['Channel_Name']['Channel_Name']
                #     )
                #     cursor.execute(query_playlist, values_playlist)
                query_playlist = """
                INSERT INTO playlist (
                    playlist_id,
                    channel_id,
                    channel_name
                    ) VALUES (%s, %s, %s)
                    """
                values_playlist = (
                    migration_data['Channel_Name']['Playlist_Id'],
                    migration_data['Channel_Name']['Channel_Id'],
                    migration_data['Channel_Name']['Channel_Name']
                )
                cursor.execute(query_channel, values_channel)
                cursor.execute(query_playlist, values_playlist)

                for video_id, video_data in migration_data.items():
                    if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
                        query_video = """
                            INSERT INTO Video (
                                video_id,
                                playlist_id,
                                video_name,
                                video_description,
                                published_date,
                                view_count,
                                like_count,
                                comment_count,
                                duration
                                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
                        values_video = (
                            video_id,
                            migration_data['Channel_Name']['Playlist_Id'],
                            video_data['Video_Name'],
                            video_data['Video_Description'],
                            video_data['PublishedAt'],
                            video_data['View_Count'],
                            video_data['Like_Count'],
                            video_data['Comment_Count'],
                            DataMigration.durationtoint(video_data['Duration'])    
                        )
                        cursor.execute(query_video, values_video)
                        
                for video_id, video_data in migration_data.items():
                    if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
                        query_Comment = """
                            INSERT INTO Comment (
                                video_id,
                                channel_id,
                                comment_text,
                                comment_author
                                )VALUES(%s,%s,%s,%s)       
                        """
                        for i in range(len(video_data['Comments'])):
                            values_comment = (
                                # video_data['Comments'][i]['Comment_Id'],
                                video_id,
                                id,
                                video_data['Comments'][i]['Comment_Text'],
                                video_data['Comments'][i]['Comment_Author']
                                # video_data['Comments'][i]['Comment_PublishedAt']
                            )
                            cursor.execute(query_Comment, values_comment)       
        db_connection.commit()    
        return "Data migrated successfully from MongoDB Atlas to MySQL data warehouse!"  
    
    def list_channel_names(): 
        cursor = db_connection.cursor()
        cursor.execute("select channel_name,channel_subscription,channel_views from channel")
        list_name = cursor.fetchall()
        # s = [i[0] for i in s]
        # s.sort(reverse=False)
        return list_name   
         
with left_column:
    st.title("YouTube Warehousing")
    with st.container():
        st.info("Checking Data Exist or not in Mongo")
        channelId = st.text_input("Channel ID")
        if channelId:
            retrieved_data = collection.find_one({'Channel_Name.Channel_Id': channelId})
    with st.container():
        st.info("Data reterving from Youtube Api")
        channel_id = st.text_input("Enter the Channel ID")
        if channel_id:
            start_time = time.time()
            youtube_data_retriever = YouTubeDataRetriever(api_key)
            channel_data = youtube_data_retriever.retrieve_channel_data(channel_id)
            end_time = time.time()
            elapsed_time = end_time - start_time
            st.write("Data fetched successfully!")
            st.write(f"Data retrieval took {elapsed_time:.2f} seconds.")
            
    with st.container():
        st.text("Store Data To MongoDb")
        try:
            if st.button("Store Data in MongoDB Atlas"):
                collection.insert_one(channel_data)
                st.write("<span style='color: green;'>Data stored successfully in MongoDB Atlas!</span>", unsafe_allow_html=True)
        except:
            st.text("Please enter channel Id to retrive data")
            

            
    with st.container():
        st.info("Migrate Data From MySQL")
        id = st.text_input("Channel ID to Migrate Data to MySQL")
        if id:
            try:
                message =  DataMigration.migratetoSql(id)
                st.success(message)
            except:
                st.write("<span style='color: red;'>Data already stored in MySQL Database</span>", unsafe_allow_html=True)            
    with st.container():
        st.info("channel analysis")
        def question1():
            cursor.execute(
                """SELECT playlist.channel_name, video.video_name FROM playlist JOIN video ON playlist.playlist_id = video.playlist_id""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'video_name']).reset_index(drop=True)
            df.index += 1
            return df

        def question2():
            cursor.execute(
                """SELECT playlist.channel_name, COUNT(video.video_id) AS video_count FROM playlist JOIN Video ON playlist.playlist_id = video.playlist_id GROUP BY playlist.channel_name ORDER BY video_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'video_count']).reset_index(drop=True)
            df.index += 1
            return df

        def question3():
            cursor.execute(
                """SELECT video.video_name, playlist.channel_name, video.view_count FROM video JOIN playlist ON video.playlist_id = playlist.playlist_id ORDER BY video.view_count DESC LIMIT 10;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['Video_name', 'Channel_name', 'View count']).reset_index(drop=True)
            df.index += 1
            return df

        def question4():
            cursor.execute("""SELECT Video_name, comment_count from video ORDER BY comment_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['video Name', 'comment count']).reset_index(drop=True)
            df.index += 1
            return df

        def question5():
            cursor.execute(
                """SELECT Video.video_name, playlist.channel_name, Video.like_count FROM Video JOIN playlist ON video.playlist_id = playlist.playlist_id ORDER BY video.like_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['video_name', 'Channel_name', 'like_count']).reset_index(drop=True)
            df.index += 1
            return df

        def question6():
            cursor.execute(
                """SELECT video_name, like_count FROM video ORDER BY like_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['video_name', 'like_count']).reset_index(drop=True)
            df.index += 1
            return df

        def question7():
            cursor.execute("""SELECT channel_name, channel_views FROM channel ORDER BY channel_views DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'total_number_of_views']).reset_index(drop=True)
            df.index += 1
            return df

        def question8():
            cursor.execute(
                """SELECT playlist.channel_name, Video.video_name, Video.published_date FROM playlist JOIN video ON playlist.playlist_id = Video.playlist_id WHERE EXTRACT(YEAR FROM video.published_date) = 2022;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'video_name', 'Year_2022']).reset_index(drop=True)
            df.index += 1
            return df
        
        def question9():
            cursor.execute(
                """SELECT playlist.channel_name, AVG(Video.duration) AS average_duration FROM playlist JOIN video ON playlist.playlist_id = Video.playlist_id GROUP BY playlist.channel_name;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'average_duration_of_videos']).reset_index(drop=True)
            df['average_duration_of_videos'] = df['average_duration_of_videos'].astype(float)
            df['average_duration_of_videos'] = df['average_duration_of_videos'].round(2)
            df.index += 1
            return df

        def question10():
            cursor.execute("""SELECT playlist.channel_name, Video.video_name, Video.comment_count FROM playlist JOIN Video ON playlist.playlist_id = Video.playlist_id where Video.comment_count>0 ORDER BY comment_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'video_name', 'number_of_comments']).reset_index(drop=True)
            df.index += 1
            return df
        def que10():
            cursor.execute("""SELECT playlist.channel_name, Video.video_name, Video.comment_count FROM playlist JOIN Video ON playlist.playlist_id = Video.playlist_id where Video.comment_count>=100 ORDER BY comment_count DESC;""")
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['channel_name', 'video_name', 'number_of_comments']).reset_index(drop=True)
            df.index += 1
            return df
            
    query_options = ['Tap view', '1. What are the names of all the videos and their corresponding channels?',
                     '2. Which channels have the most number of videos, and how many videos do they have?',
                     '3. What are the top 10 most viewed videos and their respective channels?',
                     '4. How many comments were made on each video, and what are their corresponding video names?',
                     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                     '6. What is the total number of likes for each video, and what are their corresponding video names?',
                     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                     '8. What are the names of all the channels that have published videos in the year 2022?',
                     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                     '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
    
    select_question = st.selectbox("select the squestion", query_options)
    if select_question == '1. What are the names of all the videos and their corresponding channels?':
        st.dataframe(question1())
    elif select_question == '2. Which channels have the most number of videos, and how many videos do they have?':
        st.dataframe(question2())
        if st.button("Visulization"):
            result = "Two"
    elif select_question == '3. What are the top 10 most viewed videos and their respective channels?':
        st.dataframe(question3())
        if st.button("Visulization"):
            result = "Three"
    elif select_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        st.dataframe(question4())
    elif select_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.dataframe(question5())
    elif select_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.dataframe(question6())
    elif select_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        st.dataframe(question7())
        if st.button("Visulization"):
            result = "seven"
    elif select_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        st.dataframe(question8())
    elif select_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.dataframe(question9())
        if st.button("Visulization"):
            result = "nine"
    elif select_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        st.dataframe(question10())    
        if st.button("Visulization"):
            result = "ten"
            

with right_column:
    try:
        with st.container():
            st.info("Channel Information")
            st.write("Channel Name:", channel_data['Channel_Name']['Channel_Name'])
            st.write("Channel ID:", channel_data['Channel_Name']['Channel_Id'])
            st.write("Subscription Count:", channel_data['Channel_Name']['Subscription_Count'])
            st.write("Channel Views:", channel_data['Channel_Name']['Channel_Views'])
            st.write("Channel Description:", channel_data['Channel_Name']['Channel_Description'])
    except:
        pass
    with st.container():
        data = DataMigration.list_channel_names()
        if not data:
            st.info("The SQL database is currently empty")
        else:
            st.info("List of channels in SQL database")
            df = pd.DataFrame(data, columns=['channel_name', 'channel_subscription', 'channel_views'])
            df.index+=1
            st.dataframe(df)
            fig = px.pie(df, values='channel_subscription', names='channel_name', title="YouTube Channel Subscriptions")
            st.plotly_chart(fig)       
        
    try:
        with st.container():
            st.info("Data from MongoDb")
            if retrieved_data:
                st.subheader("Retrieved Data:")
                st.write("Channel Name:", retrieved_data['Channel_Name']['Channel_Name'])
                st.write("Subscribers:", retrieved_data['Channel_Name']['Subscription_Count'])
            else:
                st.warning("Data not found in MongoDB Atlas!") 
    except:
        pass 
    try:
        with st.container():
            if result == "Two":
                result1 = question2()  # Call the correct function to retrieve data
                fig = px.bar(result1, x='channel_name', y='video_count', labels={'channel_name': 'Channel Name', 'video_count': 'Video Count'})
                st.plotly_chart(fig)
            elif result == "Three":
                result2 = question3()
                fig = px.pie(result2,values='View count',names='Video_name',hover_data=['Channel_name'], labels={'Channel_name':'Channel Name'})
                st.plotly_chart(fig)
            elif result == "seven":
                result3 = question7()
                fig = px.bar(result3,x='channel_name',y='total_number_of_views',labels={'channel_name':'Channel Name','total_number_of_views':'Totel Views'})
                st.plotly_chart(fig)
            elif result == "nine":
                result4 = question9()
                fig = px.pie(result4,values='average_duration_of_videos',names='channel_name')
                st.plotly_chart(fig)
            elif result == "ten":
                result5 = que10()
                fig = px.scatter(result5,y='video_name',x='number_of_comments',symbol='channel_name')
                st.plotly_chart(fig)

    except:
        pass    
