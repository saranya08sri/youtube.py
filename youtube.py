from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection

def Api_connect():
    Api_Id="AIzaSyA8t0lDKz46MWRzWGtPfcPVi8jlqe03UvY"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube

youtube=Api_connect()


#get channel information

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute() 

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subscribers=i['statistics']['subscriberCount'],
              Views=i['statistics']['viewCount'],
              Total_Videos=i['statistics']["videoCount"],
              Channel_Description=i["snippet"]["description"],
              Playlist_Id=i['contentDetails']["relatedPlaylists"]["uploads"])
    return data


#get vedio ids

def get_video_ids(channel_id):
    video_ids=[]
    try:
        response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
        Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token=None

        while True:
            response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
            for item in response1.get('items', []):
                    video_ids.append(item['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')

            if next_page_token is None:
                break
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return video_ids


#get video information

def get_video_info(Video_Ids):
    video_data=[]
    try:
        for video_id in Video_Ids:
            request=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response=request.execute()
            
            for item in response.get('items', []):
                    data = {
                        'channel_Name': item['snippet']['channelTitle'],
                        'channel_Id': item['snippet']['channelId'],
                        'Video_Id': item['id'],
                        'Title': item['snippet']['title'],
                        'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                        'Description': item['snippet'].get('description', ''),
                        'published_Date': item['snippet']['publishedAt'],
                        'Duration': item['contentDetails']['duration'],
                        'View_count': item['statistics'].get('viewCount', 0),
                        'Like_count': item['statistics'].get('likeCount', 0),
                        'Favorite_count': item['statistics'].get('favoriteCount', 0),
                        'Comment_count': item['statistics'].get('commentCount', 0),
                        'Tags': item['snippet'].get('tags')
                    }
                    video_data.append(data)
               
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return video_data
        
        
#get Playlist Detalis

def get_playlist_details(channel_id):
    next_page_token=None
    Playlist=[]
    while True:
        request=youtube.playlists().list(
                            part="snippet,ContentDetails",
                            channelId=channel_id,
                            maxResults=50,
                            pageToken=next_page_token
            )
        response=request.execute()

        for i in response['items']:
            data=dict(Playlist_Id=i['id'],
                    Title=i['snippet']['title'],
                    Channel_Id=i['snippet']['channelId'],
                    Channel_Name=i['snippet']['channelTitle'],
                    PublishedAt=i['snippet']['publishedAt'],
                    Video_Count=i['contentDetails']['itemCount'])
            Playlist.append(data)
        
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist
    
    
#get comment information
 
def get_comment_info(comment):
    Comment_data=[]
    try:
        for video_id in comment:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                        Video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
        pass
    return Comment_data

         
#upload data to mongodb

myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb=myclient["Youtube_data"]

# Data transfer to mongoDB

def channel_details(channel_id):
    channel_det=get_channel_info(channel_id)
    play_det=get_playlist_details(channel_id)
    vid_ids=get_video_ids(channel_id)
    video_det=get_video_info(vid_ids)
    comment_det=get_comment_info(vid_ids)
    
    mycoll=mydb["channel_detalis"]
    mycoll.insert_one({"channel_information":channel_det,"playlist_information":play_det,
                       "video_information": video_det,"comment_information": comment_det})
    
    return "successfully uploaded"
    

# Establish a connection to the MySQL database

# Create and insert a data into table for channel

def Channel_table():
    db_connection=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="admin@081828",
                            database="ytsqldatabase",
                            port="5432")
    cursor=db_connection.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    db_connection.commit()

    
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
                                                            
    cursor.execute(create_query)
    db_connection.commit()
    
    
        
    ch_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for ch_data in mycoll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)
        
        
    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        cursor.execute(insert_query,values)
        db_connection.commit()

# create and insert a data  into table for Playlists

def playlist_table():
    db_connection=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="admin@081828",
                                database="ytsqldatabase",
                                port="5432")
    cursor=db_connection.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    db_connection.commit()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    cursor.execute(create_query)
    db_connection.commit()

    pl_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for pl_data in mycoll.find({},{"_id":0,"playlist_information":1}):
         for playlist_info in pl_data.get('playlist_information', []):
            pl_list.append(playlist_info)
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'])
        
        cursor.execute(insert_query,values)
        db_connection.commit()
        
# create and insert a data into table for video

def video_table():
        db_connection=psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="admin@081828",
                                        database="ytsqldatabase",
                                        port="5432")
        cursor=db_connection.cursor()

        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        db_connection.commit()

        create_query='''create table if not exists videos(channel_Name varchar(100),
                                                        channel_Id varchar(100),
                                                        Video_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Thumbnail varchar(100),
                                                        Description text,
                                                        published_Date timestamp,
                                                        Duration interval,
                                                        View_count bigint,
                                                        Like_count bigint,
                                                        Favorite_count int,
                                                        Comment_count int,
                                                        Tags text)'''

        cursor.execute(create_query)
        db_connection.commit()

        vi_list=[]
        mydb=myclient["Youtube_data"]
        mycoll=mydb["channel_detalis"]
        for vi_data in mycoll.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)
                
    


        for index,row in df2.iterrows():
                insert_query='''insert into videos(channel_Name,
                                                        channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Thumbnail,
                                                        Description,
                                                        published_Date,
                                                        Duration,
                                                        View_count,
                                                        Like_count,
                                                        Favorite_count,
                                                        Comment_count,
                                                        Tags)
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                values=(row['channel_Name'],
                        row['channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Thumbnail'],
                        row['Description'],
                        row['published_Date'],
                        row['Duration'],
                        row['View_count'],
                        row['Like_count'],
                        row['Favorite_count'],
                        row['Comment_count'],
                        row['Tags'])
                        
                cursor.execute(insert_query,values)
                db_connection.commit()

# create and insert a data into table for comment

def comment_table():
    db_connection=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="admin@081828",
                                    database="ytsqldatabase",
                                    port="5432")
    cursor=db_connection.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    db_connection.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_id varchar(50),
                                                    Comment_text text,
                                                    Comment_author varchar(150),
                                                    Comment_Published timestamp)'''

    cursor.execute(create_query)
    db_connection.commit()

    comm_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for comm_data in mycoll.find({},{"_id":0,"comment_information":1}):
            for comment_info in comm_data.get('comment_information', []):
                    comm_list.append(comment_info)
    df3=pd.DataFrame(comm_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                            Video_id,
                                            Comment_text,
                                            Comment_author,
                                            Comment_Published)
                                                    
                                            values(%s,%s,%s,%s,%s)'''
            values=(row['Comment_Id'],
                    row['Video_id'],
                    row['Comment_text'],
                    row['Comment_author'],
                    row['Comment_Published'])
                    

            cursor.execute(insert_query,values)
            db_connection.commit()

#  creating a function for all table
def tables():
    Channel_table()
    playlist_table()
    video_table()
    comment_table()
    
    return "Tables created successfully"


def show_channel_table():
    ch_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for ch_data in mycoll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df
    
    
def show_playlist_table():
    pl_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for pl_data in mycoll.find({},{"_id":0,"playlist_information":1}):
            for playlist_info in pl_data.get('playlist_information', []):
                pl_list.append(playlist_info)
    df1=st.dataframe(pl_list)
    return df1

def show_video_table():
    vi_list=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for vi_data in mycoll.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    return df2

def show_comment_table():
        comm_list=[]
        mydb=myclient["Youtube_data"]
        mycoll=mydb["channel_detalis"]
        for comm_data in mycoll.find({},{"_id":0,"comment_information":1}):
                for comment_info in comm_data.get('comment_information', []):
                        comm_list.append(comment_info)
        df3=st.dataframe(comm_list)
        return df3


# Streamlit code

st.markdown("<h1 style='text-align: center, color: green;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)

Channel_id=st.text_input("ENTER THE CHANNEL ID")

if st.button("Extract to momgodb"):
    ch_ids=[]
    mydb=myclient["Youtube_data"]
    mycoll=mydb["channel_detalis"]
    for ch_det in mycoll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_det["channel_information"]["Channel_Id"])
    
    if Channel_id in ch_ids:
        st.success("The Given channel id is already exists")
    else:
        insert=channel_details(Channel_id)
        st.success(insert)
        
if st.button("Migrate to sql"):
    Tables=tables()
    st.success(Tables)

show_table=st.radio("SELECT AND VIEW THE TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()

#sql connection

db_connection=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="admin@081828",
                                database="ytsqldatabase",
                                port="5432")
cursor=db_connection.cursor()

Questions=st.selectbox("select yuor question",("1. All the videos and the channels",
                                               "2. Channels with most number of videos",
                                               "3. Top 10 most viewed Videos",
                                               "4. Comments for each video and their coresponding video names",
                                               "5. Videos with highest likes and their coresponding channel name",
                                               "6. Numbers of like and Dislike in videos",
                                               "7. Number of views in each channel",
                                               "8. videos published in the year of 2022",
                                               "9. Average videos of Duration of all videos",
                                               "10. Videos have Highest number of comments"))

if Questions=="1. All the videos and the channels":
    query1='''select title videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    db_connection.commit()
    q1=cursor.fetchall()
    df=pd.DataFrame(q1,columns=["videos title","channel name"])
    st.write(df)
    
elif Questions=="2. Channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    db_connection.commit()
    q2=cursor.fetchall()
    df2=pd.DataFrame(q2,columns=["channel name","No of videos"])
    st.write(df2)
    
elif Questions=="3. Top 10 most viewed Videos":
    query3='''select view_count as views, channel_name as channelname,title as videotitle from videos
                where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    q3=cursor.fetchall()
    df3=pd.DataFrame(q3,columns=["views","channel name","videotitle"])
    st.write(df3)
    
elif Questions== "4. Comments for each video and their coresponding video names":
    query4='''select comment_count as no_comments,title as videotitle from videos where comment_count is not null'''
    cursor.execute(query4)
    db_connection.commit()
    q4=cursor.fetchall()
    df4=pd.DataFrame(q4,columns=["no of comments","videotitle"])
    st.write(df4)
    
elif Questions== "5. Videos with highest likes and their coresponding channel name":
    query5='''select title as videotitle,channel_name as channelname,like_count as likecount
                from videos where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    db_connection.commit()
    q5=cursor.fetchall()
    df5=pd.DataFrame(q5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif Questions== "6. Numbers of like and Dislike in videos":
    query6='''select like_count as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    db_connection.commit()
    q6=cursor.fetchall()
    df6=pd.DataFrame(q6,columns=["likecount","videotitle"])
    st.write(df6)
    
elif Questions== "7. Number of views in each channel":
    query7='''select channel_name as channelname,views as totalviwes from channels'''
    cursor.execute(query7)
    db_connection.commit()
    q7=cursor.fetchall()
    df7=pd.DataFrame(q7,columns=["channel name","totalviews"])
    st.write(df7)
    
elif Questions== "8. videos published in the year of 2022":
    query8='''select  title as video_title,published_date as videorelese,channel_name as channelname from videos 
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    db_connection.commit()
    q8=cursor.fetchall()
    df8=pd.DataFrame(q8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif Questions=="9. Average videos of Duration of all videos":
    query9='''select channel_name as channelname,AVG(duration)as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    db_connection.commit()
    q9=cursor.fetchall()
    df9=pd.DataFrame(q9,columns=["channelname","averageduration"])
    

    Q9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        Q9.append({'channeltitle':channel_title,'avgduration':average_duration_str})
    df1=pd.DataFrame(Q9)
    st.write(df1)

elif Questions=="10. Videos have Highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comment_count as comments from videos 
                    where comment_count is not null order by comments desc'''
    cursor.execute(query10)
    db_connection.commit()
    q10=cursor.fetchall()
    df10=pd.DataFrame(q10,columns=["videotitle","channel name","comments"])
    st.write(df10)










        
 
                                                    
    


        
        

            


    
            

                                                  

    

    