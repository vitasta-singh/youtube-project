from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from pymongo import MongoClient

# api connection
def Api_connect():
    Api_Id = 'AIzaSyBMUD52Jov6a1LBkSCvZvyay4Ih0-7GPPk'  #api key

    api_service_name = 'youtube'
    api_version = 'v3'

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube


youtube = Api_connect()
 
# function to fetch channel details
def channel_details(channel_id):
  
  response = youtube.channels().list(id=channel_id, part='snippet,statistics,contentDetails')
  channel_data = response.execute()
  for i in range(len(channel_data['items'])):
    details = dict(channel_name = channel_data['items'][i]['snippet']['title'],
        channel_ID= channel_data['items'][i]['id'],               
        channel_description = channel_data['items'][i]['snippet']['description'],
        channel_publish = channel_data['items'][i]['snippet']['publishedAt'],
        playlists = channel_data['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
        channel_scount= channel_data['items'][i]['statistics']['subscriberCount'],
        channel_vcount= channel_data['items'][i]['statistics']['videoCount'],
        channel_views= channel_data['items'][i]['statistics']['viewCount'])
  return details

#function to fetch video ids
def get_channel_videos(channel_id):
  video_ids = []
  # get Uploads playlist id
  res = youtube.channels().list(id=channel_id, 
                                part='contentDetails').execute()
  playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token = None
  
  while True:
      res = youtube.playlistItems().list( 
                                          part = 'snippet',
                                          playlistId = playlist_id, 
                                          maxResults = 50,
                                          pageToken = next_page_token).execute()
      
      for i in range(len(res['items'])):
          video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token = res.get('nextPageToken')
      
      if next_page_token is None:
          break
  return video_ids

#function to fetch video details
def get_video_details(video_Ids):
  all_video_stats=[]
  for video_ids in video_Ids:
    request = youtube.videos().list(part='snippet,statistics,contentDetails', id=video_ids)

    response= request.execute()
    for video in range (len(response['items'])):
      video_stats = dict(videoid=response['items'][video]['id'],
                               channel_name=response['items'][video]['snippet']['channelTitle'],
                               channel_ID=response['items'][video]['snippet']['channelId'],
                               videotitle=response['items'][video]['snippet']['title'],
                               videdescript=response['items'][video]['snippet']['description'],
                               videoTags=','.join(response['items'][video]['snippet'].get('tags',[])),
                               videopublish=response['items'][video]['snippet']['publishedAt'],
                               videduration= response['items'][video]['contentDetails']['duration'],
                               videolikecount=response['items'][video]['statistics'].get('likeCount',0),
                               videocommcount=response['items'][video]['statistics'].get('commentCount',0),
                               videocaption=response['items'][video]['contentDetails']['caption'],
                               videothumb=response['items'][video]['snippet']['thumbnails']['default']['url'],
                               videoVIEWcount=response['items'][video]['statistics']['viewCount'])
      all_video_stats.append(video_stats)
    
  return all_video_stats

#function to fetch comments details
def commdetails(video_Ids):
  c=[]
  for vi_deo_id in video_Ids:
    try:
      request = youtube.commentThreads().list(part="snippet",maxResults=25, videoId=vi_deo_id)
      response = request.execute()
      for commentss in response['items']:
        comm_stats = dict(videoid=commentss['snippet']['topLevelComment']['snippet']['videoId'], commid=commentss['snippet']['topLevelComment']['id'],commpublish=commentss['snippet']['topLevelComment']['snippet']['publishedAt'],commauthor=commentss['snippet']['topLevelComment']['snippet']['authorDisplayName'], 
                                  commtext=commentss['snippet']['topLevelComment']['snippet']['textDisplay'])
        c.append( comm_stats)
    except:
      pass
    
  return c

#connection for mongodb

client=MongoClient("mongodb://localhost:27017/")
db=client['youtube_DATA']
# defining main function
 
def main(channel_id):
    
  channels=channel_details(channel_id)
  videooids=get_channel_videos(channel_id)
  videodetails=get_video_details(videooids)
  commentdetail=commdetails(videooids)
  c1=db['channels']
  c1.insert_one({'channel_info':channels, 'video_details':videodetails, 'comment_details':commentdetail})
  return "successful data to mongo"
  

# SQL connection

mydb = pymysql.connect(
   host="localhost",
   user="root",
   password="Chunnu@123",database="youtube_harvesting_data")
mycursor = mydb.cursor()
client=MongoClient("mongodb://localhost:27017/")
db=client['youtube_DATA_project']
c1=db['channels']
#create new database in sql
mycursor.execute("CREATE DATABASE if not exists youtube_harvesting_data")

#channels tables
def channels_table():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")
  mycursor = mydb.cursor()
  create_query='''CREATE TABLE  if not exists channels(channel_ID VARCHAR(255) primary key, channel_name VARCHAR(255), channel_description TEXT, playlists VARCHAR(80), channel_vcount bigint, channel_scount bigint, channel_views bigint,channel_publish VARCHAR(100))'''
  mycursor.execute(create_query)
  mydb.commit() 
   
  channel_list=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for chdata in c1.find({},{"_id":0,"channel_info":1}):
    channel_list.append(chdata["channel_info"])
  df=pd.DataFrame(channel_list)

  for index,row in df.iterrows():
    insert_query='''insert into channels(channel_ID, channel_name, channel_description, playlists, channel_vcount, channel_scount, channel_views,channel_publish )
    values(%s,%s,%s,%s,%s,%s,%s,%s)'''
    values=(row['channel_ID'],row['channel_name'],row['channel_description'],row['playlists'], row['channel_vcount'],row['channel_scount'],row['channel_views'],row['channel_publish'])
    
    mycursor.execute(insert_query,values)
  mydb.commit()
   
  
  
def other_channels_table():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")
  mycursor = mydb.cursor()
  channel_list=[]
  for chdata in c1.find({},{"_id":0,"channel_info":1}):
      channel_list.append(chdata["channel_info"])
  df=pd.DataFrame(channel_list)

  for index,row in df.iterrows():
    if row['channel_ID'] == temp_channel_ID:
      # Check if the channel_id already exists in the MySQL table
      check_query = "SELECT COUNT(*) FROM channels WHERE channel_ID= %s"
      mycursor.execute(check_query, (row['channel_ID']))
      result = mycursor.fetchone()
      if result[0] == 0:
        insert_query='''insert into channels(channel_ID, channel_name, channel_description, playlists, channel_vcount, channel_scount, channel_views,channel_publish ) values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_ID'],row['channel_name'],row['channel_description'],row['playlists'], row['channel_vcount'],row['channel_scount'],row['channel_views'],row['channel_publish'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()
   
        
        return 'channel data inserted successfully'
      else:
        return "channel data already exist"
                


        
        
      

#video tables
def video_tables():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")
  mycursor = mydb.cursor()
  create_query='''CREATE TABLE  if not exists videodetails(channel_name VARCHAR(80),
                                                            videoid VARCHAR(80) PRIMARY KEY ,
                                                            channel_ID VARCHAR(100),
                                                            videotitle VARCHAR(255),
                                                            videdescript TEXT,
                                                            videopublish TIMESTAMP,
                                                            videocaption VARCHAR(80),
                                                            videoTags TEXT,
                                                            videduration VARCHAR(100),
                                                            videocommcount bigint,
                                                            videothumb VARCHAR(255),
                                                            videolikecount int,
                                                            videoVIEWcount bigint)'''
  mycursor.execute(create_query)
  mydb.commit()
      

  db=client['youtube_DATA_project']
  c1=db['channels']
  video_details=[]
  for vidata in c1.find({},{"_id":0,"video_details":1}):
      for i in range(len(vidata["video_details"])):
          video_details.append(vidata["video_details"][i])
  df_=pd.DataFrame(video_details)  
  df_['videolikecount'] = df_['videolikecount'].apply(lambda x: int(x) if str(x).isdigit() else 0) 
  for index,row in df_.iterrows():
      insert_query='''insert into videodetails(channel_name, videoid, channel_ID, videotitle, videdescript, videopublish, videocaption, videoTags, videduration, videocommcount,videothumb,videolikecount, videoVIEWcount)
      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
      values=(row['channel_name'],row['videoid'],row['channel_ID'],row['videotitle'],row['videdescript'],row['videopublish'].replace("T"," ").replace("Z"," "),row['videocaption'],row['videoTags'],row['videduration'].replace('PT', ' ').replace('H', ':').replace('M', ':').split('S')[0],row['videocommcount'],row['videothumb'],int(row['videolikecount']),row['videoVIEWcount'])
      
      mycursor.execute(insert_query,values)
      
  mydb.commit() 
  

  
 
def other_video_table():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")
  mycursor = mydb.cursor()
  video_details=[]
  for vidata in c1.find({},{"_id":0,"video_details":1}):
    for i in range(len(vidata["video_details"])):
      video_details.append(vidata["video_details"][i])
  df_=pd.DataFrame(video_details)   
  for index,row in df_.iterrows():
    if row['channel_ID']==temp_channel_ID:
      temp_videoid.append(row['videoid'])
       # Check if the video_id already exists in the MySQL table
      check_query = "SELECT COUNT(*) FROM videodetails WHERE videoid = %s"
      mycursor.execute(check_query,(row['videoid']))
      result = mycursor.fetchone()
      if result[0] == 0:
        insert_query='''insert into videodetails(channel_name, videoid, channel_ID, videotitle, videdescript, videopublish, videocaption, videoTags, videduration, videocommcount,videothumb,videolikecount, videoVIEWcount)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],row['videoid'],row['channel_ID'],row['videotitle'],row['videdescript'],row['videopublish'].replace("T"," ").replace("Z"," "),row['videocaption'],row['videoTags'],row['videduration'].replace('PT', ' ').replace('H', ':').replace('M', ':').split('S')[0],row['videocommcount'],row['videothumb'],int(row['videolikecount']),row['videoVIEWcount'])
        
        mycursor.execute(insert_query,values)
        m="new video inserted"              
      else:
        m="video data already exist"
  mydb.commit()
  return m      

  
    
      

#comment table
def comment_table():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")

  mycursor = mydb.cursor()
  create_query='''CREATE TABLE  if not exists comments_details(commid VARCHAR(100) PRIMARY KEY,videoid VARCHAR(100), commtext TEXT, commauthor VARCHAR(100), commpublish VARCHAR(100))'''
  mycursor.execute(create_query)
  mydb.commit()
  comment_details=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for comm_data in c1.find({},{"_id":0,"comment_details":1}):
    for i in range(len(comm_data["comment_details"])):
      comment_details.append(comm_data["comment_details"][i])
  df2=pd.DataFrame(comment_details)
      
  for index,row in df2.iterrows():
    insert_query='''insert into comments_details(commid, videoid, commtext, commauthor, commpublish)
    values(%s,%s,%s,%s,%s)'''
    values=(row['commid'],row['videoid'],row['commtext'],row['commauthor'],row['commpublish'].replace("T"," ").replace("Z"," "))
    
    mycursor.execute(insert_query,values)
  mydb.commit()


def other_comment_table():
  mydb = pymysql.connect(
  host="localhost",
  user="root",
  password="Chunnu@123",database="youtube_harvesting_data")
  mycursor = mydb.cursor()
  comment_details=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for comm_data in c1.find({},{"_id":0,"comment_details":1}):
    for i in range(len(comm_data["comment_details"])):
      comment_details.append(comm_data["comment_details"][i])
  df2=pd.DataFrame(comment_details)
      
  for index,row in df2.iterrows():
    if row['videoid'] in temp_videoid:
      # Check if the comment_Id already exists in the MySQL table
      check_query = "SELECT COUNT(*) FROM comments_details WHERE commid = %s"
      mycursor.execute(check_query, (row['commid']))
      result = mycursor.fetchone()
      if result[0] == 0:
        insert_query='''insert into comments_details(commid, videoid, commtext, commauthor, commpublish) values(%s,%s,%s,%s,%s)'''
        values=(row['commid'],row['videoid'],row['commtext'],row['commauthor'],row['commpublish'].replace("T"," ").replace("Z"," "))
        
        mycursor.execute(insert_query,values)
        m="new comments inserted"              
      else:
        m="comments data already exist"
  mydb.commit()
  return m      


           
        


       
       

#combine tables function
def tables():
    CH=other_channels_table()
    st.write(CH)
    VI=other_video_table()
    st.write(VI)
    CO=other_comment_table()
    st.write(CO)
    return "Tables Created successfully"


#channel table in streamlit
def view_channel_table():
  channel_list=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for chdata in c1.find({},{"_id":0,"channel_info":1}):
      channel_list.append(chdata["channel_info"])
  ch_table=st.dataframe(channel_list)
  return ch_table



 #video table in st                     
def view_video_table():
  video_details=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for vidata in c1.find({},{"_id":0,"video_details":1}):
    for i in range(len(vidata["video_details"])):
      video_details.append(vidata["video_details"][i])
  vi_table=st.dataframe(video_details)
  return vi_table



#comments in streamlit
def view_comments_table():
  comment_details=[]
  db=client['youtube_DATA_project']
  c1=db['channels']
  for comm_data in c1.find({},{"_id":0,"comment_details":1}):
    for i in range(len(comm_data["comment_details"])):
      comment_details.append(comm_data["comment_details"][i])
  comm_table=st.dataframe(comment_details)
  return comm_table

# title n side column of streamlit
st.title(':blue[YouTube Data Harvesting and Warehousing] ')
channel_ID=st.text_input("type channel id")
temp_channel_ID =channel_ID
temp_videoid = []
channels = channel_ID.split(',')
channels = [ch.strip() for ch in channels if ch]
if st.button('show channel data'):
    channeldata = channel_details(channel_ID)
    st.success(channeldata)

with st.sidebar:
    
    st.markdown("## :violet[Skills take away From This Project] : Python scripting, API integration, Data Collection in MongoDB, SQL,Streamlit") 
    st.markdown("## :violet[Overall view] : Building a simple UI with Streamlit, retrieving data from YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
    st.markdown("## :violet[Developed by] : Vitasta Singh")
col1, col2 = st.columns(2)    

 #Collection and storing to mongodb via streamlit  
with col1: 
   if st.button("COLLECTION & STORING IN MONGODB"):
      for channel in channels:
        ch_list=[]
        db=client['youtube_DATA_project']
        c1=db['channels']
        
        for chdata in c1.find({},{"_id":0,"channel_info":1}):
            ch_list.append(chdata["channel_info"]["channel_ID"])
        if channel_ID in ch_list:
            st.success("channel details already exist")
        else:
            insert= main(channel_ID)
            st.success(insert)
         

#migration to SQL
with col2:
  if st.button("MIGRATE TO SQL") and temp_channel_ID !="":
    stream_table=tables()
    st.success(stream_table)
  
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":red[videos]",":blue[comments]"))
if temp_channel_ID != "":
  if show_table == ":green[channels]":
    st.success(view_channel_table())
    

  elif show_table ==":red[videos]":
    st.success(view_video_table())
    
  elif show_table == ":blue[comments]":
    st.success(view_comments_table())
    

    #connection with SQL
mydb = pymysql.connect(
   host="localhost",
   user="root",
   password="Chunnu@123",database="youtube_harvesting_data")

mycursor = mydb.cursor()

#query question
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
if question == '1. What are the names of all the videos and their corresponding channels?':
    query1 = "select videotitle,channel_name from videodetails;"
    mycursor.execute(query1)
    
    t1=mycursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
  query2="select channel_name, channel_vcount from channels order by channel_vcount desc;"
  mycursor.execute(query2)
    
  t2=mycursor.fetchall()
  st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select videoVIEWcount, channel_name, videotitle from videodetails  order by videoVIEWcount desc limit 10;'''
    mycursor.execute(query3)
    
    t3 = mycursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))  

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select videocommcount ,videotitle from videodetails ;"
    mycursor.execute(query4)
    
    t4=mycursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select videotitle, channel_name, videolikecount from videodetails where videolikecount is not null order by videolikecount desc;'''
    mycursor.execute(query5)
    
    t5 = mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like Count"]))

elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names? ':
    query6 = '''select videolikecount, videotitle from videodetails;'''
    mycursor.execute(query6)
    
    t6 = mycursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))    

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select channel_name, ch_views from channels;"
    mycursor.execute(query7)
    
    t7=mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select videotitle, videopublish, channel_name  from videodetails 
                where extract(year from videopublish) = 2022;'''
    mycursor.execute(query8)
    
    t8=mycursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT channel_name, AVG(videduration) FROM videodetails GROUP BY channel_name;"
    mycursor.execute(query9)
    
    t9=mycursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))


elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select videotitle, channel_name, videocommcount  from videodetails 
                       where  videocommcount is not null order by videocommcount desc;'''
    mycursor.execute(query10)
    
    t10=mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
  
  



    










  
  



    









