from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import mysql.connector
import re
import streamlit as st
import json

# Connect to YouTube API
api_key = 'AIzaSyA6-wqQyanNUaQKgc0fTZtPrNq7hZJo14o'
youtube = build('youtube', 'v3', developerKey=api_key)

# Connect to MySQL
conn = mysql.connector.connect(
    
    host="localhost",
    user="root",
    password="Praveen@442",
    database='assignment'
)
cursor = conn.cursor()

# Function to get YouTube data
def get_channel_stats(youtube, channel_id):
    retrieve_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()

    for i in response["items"]:
        data = {"Channel_Name": {"Channel_Name": i['snippet'].get('title', None),
                                 "Channel_id": i.get("id", None),
                                 "Subscription_Count": i['statistics'].get('subscriberCount', None),
                                 "Channel_Views": i['statistics'].get('viewCount', None),
                                 "Channel_Description": i['snippet'].get('description', None),
                                 "playlist_id": i['contentDetails']['relatedPlaylists'].get('uploads', None)}}
        retrieve_data.append(data)
        playlist = data["Channel_Name"].get("playlist_id")

        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist,
            maxResults=50)
        response = request.execute()

        a = 1
        for i in response['items']:
            video_ids1 = i['contentDetails']['videoId']
            # for getting video details
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_ids1)
            response1 = request.execute()
            try:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_ids1
                )
                response2 = request.execute()
            except HttpError:
                continue
            comments = []

            for comment in range(len(response2["items"])):
                try:
                    Comment_id = response2["items"][comment]["snippet"]["topLevelComment"]["id"]
                    comment_text = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                    comment_Author = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"][
                        "authorDisplayName"]
                    Comment_PublishedAt = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"][
                        "publishedAt"]

                    data = {f"comments_id_{comment + 1}": {"Comment_id": Comment_id,
                                                           "comment_text": comment_text,
                                                           "comment_Author": comment_Author,
                                                           "Comment_PublishedAt": Comment_PublishedAt}}
                    comments.append(data)
                except KeyError:
                    continue

            if "items" in response1:
                video_stats = {f"video_id_{a}": dict(Video_Id=response1['items'][0].get("id", None),
                                                     Video_Name=response1['items'][0]['snippet'].get('title', None),
                                                     PublishedAt=response1['items'][0]['snippet'].get('publishedAt',
                                                                                                      None),
                                                     Video_Description=response1['items'][0]['snippet'].get(
                                                         'description', None),
                                                     Tags=response1['items'][0]['snippet'].get('tags', None),
                                                     View_Count=response1['items'][0]['statistics'].get('viewCount', 0),
                                                     Like_Count=response1['items'][0]['statistics'].get('likeCount', 0),
                                                     Comment_Count=response1['items'][0]['statistics'].get(
                                                         'commentCount', 0),
                                                     Favorite_Count=response1['items'][0]['statistics'].get(
                                                         'favoriteCount', 0),
                                                     Duration=response1['items'][0]['contentDetails'].get('duration',
                                                                                                          None),
                                                     Thumbnail=response1['items'][0]['snippet']['thumbnails'][
                                                         'default'].get('url', None),
                                                     Caption_Status=response1['items'][0]['contentDetails'].get(
                                                         'caption', None),
                                                     Comments=comments)}
                retrieve_data.append(video_stats)
                next_page_token = response.get("nextPageToken")
                a = a + 1
            else:
                continue

            while next_page_token is not None:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    maxResults=50,
                    playlistId=playlist,
                    pageToken=next_page_token
                )
                response = request.execute()

                for i in response['items']:
                    video_ids1 = i['contentDetails']['videoId']
                    request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_ids1)
                    response4 = request.execute()

                    try:

                        request = youtube.commentThreads().list(
                            part="snippet,replies",
                            videoId=video_ids1
                        )
                        response5 = request.execute()
                    except HttpError:
                        continue
                    comments = []

                    for comment in range(len(response5["items"])):
                        try:
                            Comment_id = response5["items"][comment]["snippet"]["topLevelComment"]["id"]
                            comment_text = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"][
                                "textOriginal"]
                            comment_Author = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"][
                                "authorDisplayName"]
                            Comment_PublishedAt = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"][
                                "publishedAt"]

                            data = {f"comments_id_{comment + 1}": {"Comment_id": Comment_id,
                                                                   "comment_text": comment_text,
                                                                   "comment_Author": comment_Author,
                                                                   "Comment_PublishedAt": Comment_PublishedAt}}
                            comments.append(data)
                        except KeyError:
                            continue

                    if "items" in response4:
                        video_stats = {f"video_id_{a}": dict(Video_Id=response4['items'][0].get("id", None),
                                                             Video_Name=response4['items'][0]['snippet'].get(
                                                                 'title', None),
                                                             PublishedAt=response4['items'][0]['snippet'].get(
                                                                 'publishedAt', None),
                                                             Video_Description=response4['items'][0]['snippet'].get(
                                                                 'description', None),
                                                             Tags=response4['items'][0]['snippet'].get(
                                                                 'tags', None),
                                                             View_Count=response4['items'][0]['statistics'].get(
                                                                 'viewCount', 0),
                                                             Like_Count=response4['items'][0]['statistics'].get(
                                                                 'likeCount', 0),
                                                             Comment_Count=response4['items'][0]['statistics'].get(
                                                                 'commentCount', 0),
                                                             Favorite_Count=response4['items'][0]['statistics'].get(
                                                                 'favoriteCount', 0),
                                                             Duration=response4['items'][0]['contentDetails'].get(
                                                                 'duration', None),
                                                             Thumbnail=response4['items'][0]['snippet']['thumbnails'][
                                                                 'default'].get(
                                                                 'url', None),
                                                             Caption_Status=response4['items'][0]['contentDetails'].get(
                                                                 'caption', None),
                                                             Comments=comments)}
                        retrieve_data.append(video_stats)
                        next_page_token = response.get("nextPageToken")
                        a = a + 1
                    else:
                        continue

    list_of_dicts = retrieve_data
    result_dict = {
        key: value for d in list_of_dicts for key, value in d.items()}

    return result_dict


# Function to migrate data to MySQL
def migrate_to_sql(data):
    # Convert data to DataFrame and migrate to MySQL
    df_channel_details = pd.DataFrame(
        columns=["channel_id", "channel_name", "subscription_count", "channel_views", "channel_description"])
    for i in data:
        df_channel_details.loc[0] = [i["Channel_Name"]['Channel_id'],
                                     i["Channel_Name"]['Channel_Name'],
                                     i["Channel_Name"]['Subscription_Count'],
                                     i["Channel_Name"]['Channel_Views'],
                                     i["Channel_Name"]['Channel_Description']
                                     ]
    df_channel_table = pd.DataFrame(df_channel_details)
    df_channel_table['subscription_count'] = pd.to_numeric(df_channel_table['subscription_count'])
    df_channel_table['channel_views'] = pd.to_numeric(df_channel_table['channel_views'])

    channel_values = df_channel_table.values.tolist()
    insert_into_channel_table = ("INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, channel_description) VALUES (%s, %s, %s, %s, %s)")
    cursor.executemany(insert_into_channel_table, channel_values)
    conn.commit()

    # Get playlist_id
    df_playlist_details = pd.DataFrame(columns=["Channel_id", " Playlist_id"])

    for i in data:
        df_playlist_details.loc[0] = [i["Channel_Name"]['Channel_id'],
                                      i["Channel_Name"]['playlist_id']]
    df_playlist_table = pd.DataFrame(df_playlist_details)

    playlist_values = df_playlist_table.values.tolist()
    insert_into_playlist_table = ("INSERT INTO playlist (playlist_id, channel_id) VALUES (%s, %s)")
    cursor.executemany(insert_into_playlist_table, playlist_values)
    conn.commit()

    # Get video details
    df_video_details = pd.DataFrame(columns=["Video_id", "Video_Name", "PublishedAt", "Video_Description",
                                             "Tags", "View_Count", "Like_Count", "Comment_Count", "Favorite_Count",
                                             "Duration", "Thumbnail", "Caption_Status", "playlist_id"])

    for i in data:
        for j in i.keys():
            if re.match(r"^video_id_\d+$", j):
                video = {
                    "Video_id": i[j]["Video_Id"],
                    "Video_Name": i[j]["Video_Name"],
                    "PublishedAt": i[j]["PublishedAt"],
                    "Video_Description": i[j]["Video_Description"],
                    "Tags": i[j]["Tags"],
                    "View_Count": i[j]["View_Count"],
                    "Like_Count": i[j]["Like_Count"],
                    "Comment_Count": i[j]["Comment_Count"],
                    "Favorite_Count": i[j]["Favorite_Count"],
                    "Duration": i[j]["Duration"],
                    "Thumbnail": i[j]["Thumbnail"],
                    "Caption_Status": i[j]["Caption_Status"],
                    "playlist_id": playlist_values[0][0]
                }
                df_video_details = pd.concat([df_video_details, pd.DataFrame([video])], ignore_index=True)

    df_video_table = pd.DataFrame(df_video_details)

    video_values = df_video_table.values.tolist()
    insert_into_video_table = ("INSERT INTO video (video_id, video_name, publishedAt, video_description, tags, view_count, like_count, comment_count, favorite_count, duration, thumbnail, caption_status, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(insert_into_video_table, video_values)
    conn.commit()

    # Get comments
    df_comments_details = pd.DataFrame(columns=["Comment_id", "Comment_text", "Comment_Author", "Comment_PublishedAt",
                                                "Video_id"])

    for i in data:
        for j in i.keys():
            if re.match(r"^video_id_\d+$", j):
                for comment in i[j]['Comments']:
                    comments = {
                        "Comment_id": list(comment.values())[0]['Comment_id'],
                        "Comment_text": list(comment.values())[0]['comment_text'],
                        "Comment_Author": list(comment.values())[0]['comment_Author'],
                        "Comment_PublishedAt": list(comment.values())[0]['Comment_PublishedAt'],
                        "Video_id": i[j]["Video_Id"]
                    }
                    df_comments_details = pd.concat([df_comments_details, pd.DataFrame([comments])], ignore_index=True)

    df_comments_table = pd.DataFrame(df_comments_details)

    comments_values = df_comments_table.values.tolist()
    insert_into_comments_table = ("INSERT INTO comments (comment_id, comment_text, comment_author, comment_publishedAt, video_id) VALUES (%s, %s, %s, %s, %s)")
    cursor.executemany(insert_into_comments_table, comments_values)
    conn.commit()


# Streamlit interface
st.title('YouTube Data Harvesting and Warehousing')
st.write('Enter a YouTube channel ID to retrieve data.')

channel_id = st.text_input('Channel ID')

if st.button('Retrieve Data'):
    channel_data = get_channel_stats(youtube, channel_id)
    st.write(channel_data)
    
    if st.button('Load Data into MySQL'):
        try:
            query = f"SELECT channel_name FROM channel WHERE channel_name = '{channel_id}';"
            cursor.execute(query)
            conn.commit()
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            x = df.values.tolist()
            k = x[0][0] if x else None
            
            if k and channel_id == k:
                st.warning("Duplicate channel_id, Data already exists")
            else:
                migrate_to_sql(channel_data)
                st.success("Migrated from MongoDB to SQL Successfully")
        except IndexError:
            st.warning("Please select the channel name")
        except Exception as e:
            st.error(f"Error: {e}")

# Questions and corresponding SQL queries
questions = [
    "",
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "6.What is the total number of views for each channel, and what are their corresponding channel names?",
    "7.Which videos have the highest number of comments, and what are their corresponding channel names?",
    "8.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "9.What are the names of all the channels that have published videos in the year 2022?",
    "10.What is the average duration of all videos in each channel, and what are their corresponding channel names?"
]

queries = [
    "",
    "SELECT c.video_name, A.channel_name FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id",
    "SELECT a.channel_name, COUNT(*) AS video_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id GROUP BY a.channel_name ORDER BY video_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.view_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id ORDER BY C.view_count DESC LIMIT 10",
    "SELECT c.video_name, c.comment_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id",
    "SELECT c.video_name, C.like_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id",
    "SELECT A.channel_name, SUM(C.view_count) AS total_views FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name",
    "SELECT A.channel_name,c.video_name, c.comment_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id ORDER BY c.comment_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.like_count FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id ORDER BY C.like_count DESC LIMIT 1;",
    "SELECT DISTINCT A.channel_name,YEAR(CAST(C.publishedAt AS DATE)) as year FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id WHERE YEAR(CAST(C.publishedAt AS DATE)) = 2022;",
    "SELECT A.channel_name, ROUND(AVG(TIME_TO_SEC(CAST(C.duration AS TIME))/60), 2) AS average_duration FROM channel A INNER JOIN playlist B ON A.channel_id = B.channel_id JOIN video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name;"
]

# Display the dropdown to select the question
selected_question = st.selectbox("Select Query:", questions)

# Execute the corresponding query based on the selected question
query_index = questions.index(selected_question)
query = queries[query_index]
if query:
    cursor.execute(query)
    results = cursor.fetchall()

    # Convert the results to a DataFrame
    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

    # Display the table in Streamlit
    st.subheader(selected_question)
    st.dataframe(df)
    conn.commit()
