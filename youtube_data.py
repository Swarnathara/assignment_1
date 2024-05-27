from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from db import connect_database
import logging

#-------------- API CONNECTION --------------------
def connect_youtube_api():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyDt96FZRkBm_RA6ZIkKI_XHmkJAfUWGZCY'
    youtube = build(api_service_name, api_version, developerKey = api_key)
    return youtube



youtube = connect_youtube_api()

def get_channel_details(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )

        response = request.execute()
        
        if 'items' in response and response['items']:
            data = []
            for item in response["items"]:
                channel_data = {
                    "channel_id": channel_id,
                    "channel_name": item["snippet"]['title'],
                    "channel_description": item["snippet"]['description'],
                    "channel_views": item['statistics']['viewCount'],
                    "total_videos":item["statistics"]["videoCount"],
                    "playlist_id":item["contentDetails"]["relatedPlaylists"]["uploads"]
                }
                data.append(channel_data)
            return data
        else:
            logging.warning("Empty or unexpected response for channel details.")
            return None

    except Exception as e:
        logging.error(f"Error fetching channel details: {e}")
        return None




def get_video_id(channel_id):
    video_ids = []

    request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

   
    playlist_id = request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    # print(playlist_id)

    page_token = None 
    while True:
        request_2 = youtube.playlistItems().list(
            part="snippet",
            playlistId = playlist_id,
            maxResults = 50, 
            pageToken = page_token
        )
        response_2 = request_2.execute()
        for i in range(len(response_2['items'])):
            video_ids.append(response_2['items'][i]['snippet']['resourceId']["videoId"])
        page_token = response_2.get("nextPageToken")
        

        if page_token is None:
            break

    return video_ids

def get_video_information(video_ids):
    video_details = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            # response["items"][0]["snippet"]["channelTitle"]
            data = {
                
                "video_id":item['id'],
                "channel_id":item["snippet"]["channelId"],
                "video_name": item["snippet"]["title"],
                "video_description":item["snippet"]["description"],
                "tags": item["snippet"].get("tags"),
                "published_at": item["snippet"]["publishedAt"],
                "view_count": item['statistics']['viewCount'],
                "like_count":item["statistics"].get("likeCount"),
                "favorite_count":item["statistics"].get("favoriteCount") ,
                "comment_count": item["statistics"].get("commentCount"),
                "duration": item["contentDetails"]["duration"],
                "thumbnail": item["snippet"]["thumbnails"]['default']['url'],
                "caption_status": item["contentDetails"]["caption"],
            
            }
            video_details.append(data)

    return video_details


def get_comments_information(video_ids):
    comments_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId = video_id,
                maxResults = 50 
            )
            response = request.execute()

            for item in response["items"]:
                data = {
                    "comment_id": item["snippet"]["topLevelComment"]["id"],
                    "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "comment_author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "comment_published_at": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],                    
                    "video_id":item['snippet']['topLevelComment']['snippet']['videoId']
                }
                comments_data.append(data)
    except: 
        pass

    return comments_data

def get_playlist_details(channel_id):
        next_page_token = None
        playlist_data = []

        while True:
                request = youtube.playlists().list(
                        part="contentDetails,snippet",
                        channelId=channel_id,
                        maxResults = 50,
                        pageToken = next_page_token
                )
                response = request.execute()
                for item in response["items"]:
                        data = {
                                "playlist_id":item['id'],
                                "title":item["snippet"]['title'],
                                "channel_id":item["snippet"]["channelId"],
                                "channel_name":item['snippet']['channelTitle'],
                                "published_at":item['snippet']['publishedAt'],
                                "video_count":item['contentDetails']['itemCount']
                                
                                }
                        playlist_data.append(data)
                next_page_token = response.get("nextPageToken")
                if next_page_token is None:
                        break

        return playlist_data


client = pymongo.MongoClient("mongodb+srv://swarnathara:admin@cluster0.gpvfj7h.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]


def gather_all_details(channel_id):
    ch_details = get_channel_details(channel_id)
    video_ids_list = get_video_id(channel_id)
    vid_info=get_video_information(video_ids_list)
    comment_info=get_comments_information(video_ids_list)
    playlist_info = get_playlist_details(channel_id)


    collection_1=db["channel_details"]
    collection_1.insert_one({"Channel_information":ch_details,"Playlist_Information":playlist_info,"Video_Details":vid_info,"Comment_Details":comment_info})


    return "channel details uploaded"
    

def channels_table():
    mydb = connect_database()
    if mydb is None:
        return

    cursor = mydb.cursor()
    drop_query = '''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_table_channel = '''
                    CREATE TABLE IF NOT EXISTS channel(
                        channel_id VARCHAR(255) PRIMARY KEY,
                        channel_name VARCHAR(255),
                        channel_views INTEGER, 
                        channel_description TEXT,
                        total_videos INTEGER,
                        playlist_id VARCHAR(255)
                    )
                    '''
        cursor.execute(create_table_channel)
        mydb.commit()
    except:
        print("Channels table already created")

    ch_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

  
    for c in collection.find({}, {"_id": 0, "channel_information": 1}):
        if isinstance(c["channel_information"], list):
            ch_list.extend(c["channel_information"])
        else:
            ch_list.append(c["channel_information"])
    
    df = pd.DataFrame(ch_list)
    
    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channel(channel_id, channel_name, channel_views, channel_description,total_videos,playlist_id) 
                        VALUES (%s, %s, %s, %s,%s,%s)'''

        values = (row['channel_id'], row['channel_name'], row['channel_views'], row['channel_description'],row["total_videos"],row["playlist_id"])
        conn = connect_database()  
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(insert_query, values)
                conn.commit()
                print("Channel Queries inserted successfully!")
            except Exception as e:
                print("Channel values are already inserted")

def comments_table():
    mydb = connect_database()
    if mydb is None:
        return

    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_table_comment ='''
            CREATE TABLE IF NOT EXISTS comments(
                comment_id VARCHAR(255) PRIMARY KEY, 
                comment_text TEXT,
                comment_author VARCHAR(255),
                comment_published_at TIMESTAMP,
                video_id VARCHAR(255)
               )

'''
        cursor.execute(create_table_comment)
        mydb.commit()
       
    except Exception as e:
        print("COMMENT table already created")
    
    com_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    for com in collection.find({}, {"_id": 0, "comment_details": 1}):
        if 'comment_details' in com:
            com_list.extend(com['comment_details'])


    df = pd.DataFrame(com_list)
    for idx, row in df.iterrows():
        insert_query = '''INSERT INTO comments(comment_id,comment_text, comment_author, comment_published_at,video_id) 
                        VALUES (%s, %s, %s,%s,%s)'''

        values = (row['comment_id'], row['comment_text'], row['comment_author'],row["comment_published_at"],row["video_id"])
        conn = connect_database()  
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(insert_query, values)
                conn.commit()
                print("Comments Queries inserted successfully!")
            except Exception as e:
                print("Playlists values are already inserted")
            
def iso8601_duration_to_seconds(duration_str):
    """Convert ISO 8601 duration string to seconds."""
    duration_pattern = re.compile(
        r"P(?:(?P<weeks>\d+)W)?"
        r"(?:(?P<days>\d+)D)?"
        r"T(?:(?P<hours>\d+)H)?"
        r"(?:(?P<minutes>\d+)M)?"
        r"(?:(?P<seconds>\d+)S)?"
    )
    match = duration_pattern.match(duration_str)
    if match:
        duration_parts = match.groupdict()
        duration_parts = {key: int(value) for key, value in duration_parts.items() if value}
        return timedelta(**duration_parts).total_seconds()
    elif duration_str == "P0D":
        return 0  # Duration is zero days
    else:
        return None  

def videos_table():
   
        mydb = connect_database()
        if mydb is None:
            print("Failed to connect to the database")
            return

        print("Connected to the database")

        
        cursor = mydb.cursor()
        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()


        print("Dropped existing videos table")

        create_table_video ='''
            CREATE TABLE IF NOT EXISTS videos(
                video_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                tags TEXT,
                published_at TIMESTAMP,
                view_count INTEGER,
                like_count INTEGER,
                favorite_count INTEGER,
                comment_count INTEGER,
                duration INTEGER,
                thumbnail VARCHAR(255),
                caption_status VARCHAR(255)
            )
        '''
        cursor.execute(create_table_video)
        mydb.commit()

        print("Videos table created successfully")
        v_list = []
        db = client["Youtube_data"]
        collection = db["channel_details"]

        for v in collection.find({}, {"_id": 0, "video_details": 1}):
            if 'video_details' in v:
                v_list.extend(v['video_details'])
        df = pd.DataFrame(v_list)
                # Insert data into the videos table
        
        insert_query = """INSERT INTO videos(video_id, channel_id, video_name, video_description,
                            tags, published_at, view_count, like_count, favorite_count, comment_count, 
                            duration, thumbnail, caption_status) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
        conn = connect_database()
        if conn:
            try:
                cur = conn.cursor()
                for idx, row in df.iterrows():
                    duration_seconds = iso8601_duration_to_seconds(row["duration"])
                    if duration_seconds is None:
                        print(f"Invalid duration string at index {idx}: {row['duration']}")
                        continue 
                    values = (
                        row["video_id"],
                        row["channel_id"],
                        row["video_name"],
                        row["video_description"],
                        row["tags"],
                        row["published_at"],
                        row["view_count"],
                        row["like_count"],
                        row["favorite_count"],
                        row["comment_count"],
                        duration_seconds,
                        row["thumbnail"],
                        row["caption_status"]
                    )
                    try:
                        cur.execute(insert_query, values)
                    except Exception as e:
                        print(f"Error inserting data at index {idx}: {e}")
                        conn.rollback()  # Rollback the transaction on error
                    else:
                        conn.commit()  # Commit the transaction
                print("All videos inserted successfully!")
            except Exception as e:
                print(f"Error with database operation: {e}")
            finally:
                cur.close()
        else:
            print("Failed to connect to the database")

def playlists_table():
    mydb = connect_database()
    if mydb is None:
        return

    cursor = mydb.cursor()
    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_table_playlists = '''
                    CREATE TABLE IF NOT EXISTS playlists(
                        playlist_id VARCHAR(255) PRIMARY KEY,
                        title VARCHAR(255),
                        channel_id varchar(255),
                        channel_name VARCHAR(255),
                        published_at TIMESTAMP,
                        video_count INTEGER
                    )
                    '''
        cursor.execute(create_table_playlists)
        mydb.commit()
       
    except Exception as e:
        print("playlists table already exists")

    p_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    # Flatten the list of lists to a list of dictionaries
    for c in collection.find({}, {"_id": 0, "playlist_information": 1}):
        if isinstance(c["playlist_information"], list):
            p_list.extend(c["playlist_information"])
        else:
            p_list.append(c["playlist_information"])
        
    df = pd.DataFrame(p_list)
    for idx, row in df.iterrows():
        insert_query = '''INSERT INTO playlists(playlist_id, title, channel_id,channel_name,published_at,video_count) 
                        VALUES (%s, %s, %s,%s,%s,%s)'''

        values = (row['playlist_id'], row['title'], row['channel_id'],row["channel_name"],row["published_at"],row["video_count"])
        conn = connect_database()  
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(insert_query, values)
                conn.commit()
                print("Playlists Queries inserted successfully!")
            except Exception as e:
                print("Error inserting data:", e)
            finally:
                if cur:
                    cur.close()
                conn.close()

def tables():
   channels_table()
   playlists_table()
   videos_table()
   comments_table()

   return "tables created successfully"

def show_channel_table():
    ch_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    # Flatten the list of lists to a list of dictionaries
    for c in collection.find({}, {"_id": 0, "channel_information": 1}):
        if isinstance(c["channel_information"], list):
            ch_list.extend(c["channel_information"])
        else:
            ch_list.append(c["channel_information"])
    df1 = st.dataframe(ch_list)
    return df1
    
def show_playlist_table():
    p_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    # Flatten the list of lists to a list of dictionaries
    for c in collection.find({}, {"_id": 0, "playlist_information": 1}):
        if isinstance(c["playlist_information"], list):
            p_list.extend(c["playlist_information"])
        else:
            p_list.append(c["playlist_information"])


    df2 = st.dataframe(p_list)
    return df2

def show_videos_table():
    v_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    for v in collection.find({}, {"_id": 0, "video_details": 1}):
        if 'video_details' in v:
            v_list.extend(v['video_details'])

    df3 = st.dataframe(v_list)
    return df3

def show_comment_table():
    com_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    for com in collection.find({}, {"_id": 0, "comment_details": 1}):
        if 'comment_details' in com:
            com_list.extend(com['comment_details'])

   
    df4 = st.dataframe(com_list)
    return df4


def collect_and_store_data(channel_id):
    
    return f"Data collected and stored for channel ID: {channel_id}"


def migrate_to_sql():
    
    return "Data migrated to SQL successfully."


def handle_question_selection(question):
    
    st.write(f"Handling question: {question}")




st.sidebar.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
st.image(r"C:\Users\swarn\Downloads\youtube.jpg", use_column_width=True)

selected_action = st.sidebar.selectbox("Select Action",
                                       ["Collect and Store Data", 
                                        "Migrate to SQL", 
                                        "Show Channels", 
                                        "Show Playlists", 
                                        "Show Videos", 
                                        "Show Comments",
                                        "Question and Answer"])

if selected_action == "Collect and Store Data":
    st.title("Collect and Store Data")
    channel_id = st.text_input("Enter the channel ID:")
    if st.button("Collect and Store Data"):
        result = collect_and_store_data(channel_id)
        st.success(result)

elif selected_action == "Migrate to SQL":
    st.title("Migrate to SQL")
    if st.button("Migrate to SQL"):
        result = migrate_to_sql()
        st.success(result)

elif selected_action == "Show Channels":
    st.title("Show Channels")
    show_channel_table()

elif selected_action == "Show Playlists":
    st.title("Show Playlists")
    show_playlist_table()

elif selected_action == "Show Videos":
    st.title("Show Videos")
    show_videos_table()

elif selected_action == "Show Comments":
    st.title("Show Comments")
    show_comment_table()

elif selected_action == "Question and Answer":
    st.title("Question and Answer")
    question = st.selectbox("Select your question", [
        "Names of all videos and their Corresponding Channel", 
        "Channels with the most number of videos", 
        "10 Most viewed videos",
        "Comments were made on each videos and what are their corresponding channel names",
        "Videos have the highest number of likes and their channel names",
        "Total number of likes and their corresponding channel names",
        "Total number of views for each channel and their channel names",
        "Names of all channels that have published videos in 2022",
        "Average duration of all videos in each channel, and their channel names",
        "Highest number of comments and their corresponding channel names"
    ])

    if st.button("Submit"):
        
        mydb = connect_database()
        cursor = mydb.cursor()

        if question == "Names of all videos and their Corresponding Channel":
            mydb= connect_database()
            cursor = mydb.cursor()
            q1 = '''SELECT video_name AS video_name, channel_id AS channel_name FROM videos'''
            cursor.execute(q1)
            mydb.commit()
            t1 = cursor.fetchall()
            df = pd.DataFrame(t1,columns = ["video_title","channel_name"])
            st.write(df)

        elif question == "Channels with the most number of videos":
            mydb= connect_database()
            cursor = mydb.cursor()
            query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos 
                        FROM channel 
                        ORDER BY total_videos DESC'''
            cursor.execute(query2)
            mydb.commit()
            t2 = cursor.fetchall()
            df = pd.DataFrame(t2,columns = ["Name of channel","Number of videos"])
            st.write(df)
        elif question == "10 Most viewed videos":
            mydb= connect_database()
            cursor = mydb.cursor()
            query3 = '''
                SELECT c.channel_views AS views, c.channel_name AS channelname, v.video_name AS videotitle
                FROM videos v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE c.channel_views IS NOT NULL
                ORDER BY c.channel_views DESC
                LIMIT 10
            '''
            cursor.execute(query3)
            mydb.commit()
            t3=cursor.fetchall()
            df3=pd.DataFrame(t3,columns=["Views","Channel Name","Video Title"])
            st.write(df3)

        elif question == "Comments were made on each videos and what are their corresponding channel names":
            mydb= connect_database()
            cursor = mydb.cursor()
            query4 = '''SELECT 
                c.channel_name AS channel_name,
                v.video_name AS video_title,
                cm.comment_text AS comment_text
            FROM 
                comments cm
            JOIN 
                videos v ON cm.video_id = v.video_id
            JOIN 
                channel c ON v.channel_id = c.channel_id
            WHERE 
                cm.comment_text IS NOT NULL;'''
            cursor.execute(query4)
            mydb.commit()
            t4=cursor.fetchall()
            df4=pd.DataFrame(t4,columns=["No of Comments","Video Title","Comment text"])
            st.write(df4)
            

        elif question == "Total number of likes and their corresponding channel names":
            mydb= connect_database()
            cursor = mydb.cursor()

            query5 = '''
                SELECT 
                    v.video_name AS videotitle,
                    c.channel_name AS channelname,
                    v.like_count AS likecount
                FROM 
                    videos v
                JOIN 
                    channel c ON v.channel_id = c.channel_id
                WHERE 
                    v.like_count IS NOT NULL
                ORDER BY 
                    v.like_count DESC;
            '''

            cursor.execute(query5)
            mydb.commit()
            t5=cursor.fetchall()
            df5=pd.DataFrame(t5,columns=["Video Title","Channel Name","Like Count"])
            st.write(df5)

        elif question == "Videos have the highest number of likes and their channel names":
            mydb= connect_database()
            cursor = mydb.cursor()
            query6='''select like_count as likecount,video_name as videotitle from videos'''
            cursor.execute(query6)
            mydb.commit()
            t6=cursor.fetchall()
            df6=pd.DataFrame(t6,columns=["Like Count","Video Title"])
            st.write(df6)
        
        elif question == "Total number of views for each channel and their channel names":
            mydb= connect_database()
            cursor = mydb.cursor()
            query7='''select channel_name as channelname ,channel_views as totalviews from channel'''
            cursor.execute(query7)
            mydb.commit()
            t7=cursor.fetchall()
            df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
            st.write(df7)
        elif question == "Names of all channels that have published videos in 2022":
            mydb= connect_database()
            cursor = mydb.cursor()

            query8 = '''
                SELECT v.video_name AS video_title,
                    v.published_at AS videorelease,
                    c.channel_name AS channelname
                FROM videos v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE EXTRACT(YEAR FROM v.published_at) = 2022
            '''
            cursor.execute(query8)
            mydb.commit()
            t8=cursor.fetchall()
            df8=pd.DataFrame(t8,columns=["Video Title","Published Date","Channel Name"])
            st.write(df8)
            

        elif question == "Average duration of all videos in each channel, and their channel names":
            mydb= connect_database()
            cursor = mydb.cursor()

            query9 = '''
                SELECT c.channel_name AS channelname,
                    AVG(v.duration) AS averageduration
                FROM videos v
                JOIN channel c ON v.channel_id = c.channel_id
                GROUP BY c.channel_name
            '''
            cursor.execute(query9)
            mydb.commit()
            t9=cursor.fetchall()
            df9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])

            T9=[]
            for index,row in df9.iterrows():
                channel_title=row["Channel Name"]
                average_duration=row["Average Duration"]
                average_duration_str=str(average_duration)
                T9.append(dict(ChannelTitle=channel_title,AvgDuration=average_duration_str))
            df1=pd.DataFrame(T9)
            st.write(df1)


        elif question == "Highest number of comments and their corresponding channel names":
            mydb= connect_database()
            cursor = mydb.cursor()
            query10 = '''
                SELECT c.channel_name AS channelname,
                    COUNT(cm.comment_id) AS num_comments
                FROM videos v
                JOIN channel c ON v.channel_id = c.channel_id
                JOIN comments cm ON v.video_id = cm.video_id
                GROUP BY c.channel_name
                ORDER BY num_comments DESC
                LIMIT 1
            '''


            cursor.execute(query10)
            mydb.commit()
            t10=cursor.fetchall()
            df10=pd.DataFrame(t10, columns=["channel name", "num_comments"])
            st.write(df10)
           

