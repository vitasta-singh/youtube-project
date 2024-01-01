import pandas as pd
from googleapiclient.discovery import build
import streamlit as st

# api connection
def Api_connect():
    Api_Id = 'AIzaSyBMUD52Jov6a1LBkSCvZvyay4Ih0-7GPPk'  #api key

    api_service_name = 'youtube'
    api_version = 'v3'

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube


youtube = Api_connect()
 
# function to fetch channel details
def channel_details(youtube,channel_id):
  
  response = youtube.channels().list(id=channel_id, part='snippet,statistics,contentDetails')
  channel_data = response.execute()
  for i in range(len(channel_data['items'])):
    details = dict(channel_name = channel_data['items'][i]['snippet']['title'],
    channel_ID= channel_data['items'][i]['id'],               
    channel_description = channel_data['items'][i]['snippet']['description'],
    channel_plyt = channel_data['items'][i]['snippet']['publishedAt'],
    playlists = channel_data['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
    channel_scount= channel_data['items'][i]['statistics']['subscriberCount'],
    channel_vcount= channel_data['items'][i]['statistics']['videoCount'],
    channel_views= channel_data['items'][i]['statistics']['viewCount'])
        
  return details

#function to fetch video ids
def get_video_ids(youtube,channel_id):
  
  video_id = []
  response = youtube.channels().list(id=channel_id, part='contentDetails')
  channel_data = response.execute()
  playlists= channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  c=True     
  next_page_token = None
  while c:
    request = youtube.playlistItems().list(part='contentDetails',playlistId=playlists,maxResults=50,pageToken=next_page_token)
    response_ = request.execute()

                # Get video IDs
    for item in response_['items']:
      video_id.append(item['contentDetails']['videoId'])
                # Check if there are more pages
      next_page_token = response_.get('nextPageToken')
      if not next_page_token:
        c=False
        
  return video_id

#function to fetch video details
def get_video_details(youtube,video_id):
  all_video_stats=[]
  for video_ids in video_id:
    request = youtube.videos().list(part='snippet,statistics,contentDetails', id=video_ids)

    response= request.execute()
    for video in range (len(response['items'])):
      video_stats = dict(vID=response['items'][video]['id'],
                         channeltitle=response['items'][video]['snippet']['channelTitle'],
                         channel_IDS=response['items'][video]['snippet']['channelId'],
                         videotitle=response['items'][video]['snippet']['title'],
                         videdescript=response['items'][video]['snippet']['description'],
                         vTags=','.join(response['items'][video]['snippet'].get('tags',[])),
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
def commdetails(youtube,video_IDS):
  c=[]
  for vi_deo_id in video_IDS:
    try:
      request = youtube.commentThreads().list(part="snippet",maxResults=25, videoId=vi_deo_id)
      response = request.execute()
      for commentss in response['items']:
        comm_stats = dict(videoI=commentss['snippet']['topLevelComment']['snippet']['videoId'], commid=commentss['snippet']['topLevelComment']['id'],commpublish=commentss['snippet']['topLevelComment']['snippet']['publishedAt'],commauthor=commentss['snippet']['topLevelComment']['snippet']['authorDisplayName'], 
                          commtxt=commentss['snippet']['topLevelComment']['snippet']['textDisplay'])
        c.append( comm_stats)
    except:
      pass
    
  return c

connection for mongodb
from pymongo import MongoClient
client=MongoClient("mongodb://localhost:27017/")
db=client['youtube_DATA']
# defining main function 
def main(channel_id):
    
    channels=channel_details(youtube,channel_id)
    videooids=get_video_ids(youtube,channel_id)
    videodetails=get_video_details(youtube,videooids)
    commentdetail=commdetails(youtube,videooids)
    newdb=db['channels']
    
    newdb.insert_one({'channel_info':channels, 'video_details':videodetails, 'comment_details':commentdetail})
    return "successful data to mongo"

# SQL connection
from sqlalchemy import create_engine
import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Chunnu@123")
mycursor=mydb.cursor()

#create new database in sql
mycursor.execute("use youtube_harvesting_data")

#channels tables
def channels_table():
   mydb = mysql.connector.connect(host="localhost",user="root",password="Chunnu@123",database="youtube_harvesting_data")
   mycursor=mydb.cursor()

   try:
      create_query='''CREATE TABLE  if not exists channels(ch_id VARCHAR(255) primary key, ch_name VARCHAR(255), ch_description TEXT,ch_play_id VARCHAR(80), ch_v_count bigint, ch_sub_count bigint,ch_view_count bigint,ch_played VARCHAR(100))'''
      mycursor.execute(create_query)
      mydb.commit() 
   
    
   except:
      st.write("table already exist")
   channel_list=[]
   db=client['youtube_DATA_project']
   newdb=db['channels']
   for chdata in newdb.find({},{"_id":0,"channel_info":1}):
      channel_list.append(chdata["channel_info"])
   df=pd.DataFrame(channel_list)

   for index,row in df.iterrows():
      insert_query='''insert into channels(ch_id,ch_name,ch_description,ch_play_id,ch_v_count,ch_sub_count,ch_view_count,ch_played)
      values(%s,%s,%s,%s,%s,%s,%s,%s)'''
      values=(row['channel_ID'],row['channel_name'],row['channel_description'],row['playlists'], row['channel_vcount'],row['channel_scount'],row['channel_views'],row['channel_plyt'])
      try:
         mycursor.execute(insert_query,values)
         mydb.commit()
      except:
         st.write("already")
    

#video tables
def video_tables():
  mydb = mysql.connector.connect(host="localhost",user="root",password="Chunnu@123",database="youtube_harvesting_data")
  mycursor=mydb.cursor()
  drop_query = "drop table if exists playlists"
  mycursor.execute(drop_query)


  mydb.commit()

  try:
    create_query='''CREATE TABLE  if not exists videodetails(channel_name VARCHAR(80),videoid VARCHAR(80) PRIMARY KEY,channelid VARCHAR(100),videotitle VARCHAR(255), videdescript TEXT, videopublish DATETIME, videoCaption VARCHAR(80),vTAgs TEXT, videduration TIME, videocomments TEXT,videothumb VARCHAR(255),videoLIKEcount TEXT, videoVIEWcount TEXT)'''
    mycursor.execute(create_query)
    mydb.commit()
  except:
    st.write("already exist table") 
  video_details=[]
  db=client['youtube_DATA_project']
  newdb=db['channels']
  for vidata in newdb.find({},{"_id":0,"video_details":1}):
    for i in range(len(vidata["video_details"])):
      video_details.append(vidata["video_details"][i])
  df_=pd.DataFrame(video_details)   
  for index,row in df_.iterrows():
    insert_query='''insert into videodetails(channel_name, videoid, channelid, videotitle, videdescript, videopublish, videoCaption, vTAgs, videduration, videocomments,videothumb,videoLIKEcount, videoVIEWcount)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    values=(row['channeltitle'],row['vID'],row['channel_IDS'],row['videotitle'],row['videdescript'],row['videopublish'].replace("T"," ").replace("Z"," "),row['videocaption'],row['vTags'],row['videduration'].replace('PT', ':').replace('H', ':').replace('M', ':').split('S')[0],row['videocommcount'],row['videothumb'],row['videolikecount'],row['videoVIEWcount'])
    try:
      mycursor.execute(insert_query,values)
      mydb.commit() 
    except:
      st.write("already exists values")    

#comment table
def comment_table():
  mydb = mysql.connector.connect(host="localhost",user="root",password="Chunnu@123",database="youtube_harvesting_data")
  mycursor=mydb.cursor()
  drop_query = "drop table if exists playlists"
  mycursor.execute(drop_query)
  mydb.commit()
try:
  create_query='''CREATE TABLE  if not exists comments_details(comment_id VARCHAR(100) PRIMARY KEY,video_id VARCHAR(100), comment_text TEXT, comment_author VARCHAR(100), comment_publish DATETIME)'''
  mycursor.execute(create_query)
  mydb.commit()
except:
  st.write("ALREADY HAS COMMENTS TABLE")
comment_details=[]
db=client['youtube_DATA_project']
newdb=db['channels']
for comm_data in newdb.find({},{"_id":0,"comment_details":1}):
  for i in range(len(comm_data["comment_details"])):
    comment_details.append(comm_data["comment_details"][i])
df2=pd.DataFrame(comment_details)

for index,row in df2.iterrows():
    insert_query='''insert into comments_details(comment_id, video_id, comment_text, comment_author, comment_publish)
    values(%s,%s,%s,%s,%s)'''
    values=(row['commid'],row['videoI'],row['commtxt'],row['commauthor'],row['commpublish'].replace("T"," ").replace("Z"," "))
    try:
      mycursor.execute(insert_query,values)
      mydb.commit()
    except:
      print("values of comments exists")  

#combine tables function
def tables():
    channels_table()
    video_tables()
    comment_table()
    return "Tables Created successfully"


#channel table in streamlit
def view_channel_table():
    channel_list=[]
    db=client['youtube_DATA_project']
    newdb=db['channels']
    for chdata in newdb.find({},{"_id":0,"channel_info":1}):
        channel_list.append(chdata["channel_info"])
    ch_table=st.dataframe(channel_list)
    return ch_table



 #video table in st                     
def view_video_table():
  video_details=[]
  db=client['youtube_DATA_project']
  newdb=db['channels']
  for vidata in newdb.find({},{"_id":0,"video_details":1}):
    for i in range(len(vidata["video_details"])):
      video_details.append(vidata["video_details"][i])
  vi_table=st.dataframe(video_details)
  return vi_table



#comments in streamlit
def view_comments_table():
    comment_details=[]
    db=client['youtube_DATA_project']
    newdb=db['channels']
    for comm_data in newdb.find({},{"_id":0,"comment_details":1}):
        for i in range(len(comm_data["comment_details"])):
            comment_details.append(comm_data["comment_details"][i])
    comm_table=st.dataframe(comment_details)
    return comm_table

# title n side column of streamlit
with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING PROJECT]")  
    st.header("Skills")
    st.caption("Python scripting, API integration")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Streamlit") 
    st.caption("SQL")

 #Collection and storing to mongodb via streamlit   
ch_ids=st.text_input("type channel id")
channels = ch_ids.split(',')
channels = [ch.strip() for ch in channels if ch]


if st.button("COLLECTION & STORING "):
  for channel in channels:
     ch_list=[]
     db=client['youtube_DATA_project']
     newdb=db['channels']
     for chdata in newdb.find({},{"_id":0,"channel_info":1}):
        ch_list.append(chdata["channel_info"]["channel_ID"])
     if ch_ids in ch_list:
        st.success("channel details already exist")
     else:
        insert= main(ch_ids)
        st.success(insert)
         

#migration to SQL
if st.button("migrate to SQL"):
  stream_table=tables()
  st.success(stream_table)
  
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
   view_channel_table()

elif show_table ==":red[videos]":
    view_video_table()
elif show_table == ":blue[comments]":
    view_comments_table()

    #connection with SQL
mydb = mysql.connector.connect(host="localhost",user="root",password="Chunnu@123",database="youtube_harvesting_data")
mycursor=mydb.cursor()

#query question
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have',
     '3. What are the top 10 most viewed videos and their respective channels',
     '4. How many comments were made on each video, and what are their corresponding video names',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names',
     '7. What is the total number of views for each channel, and what are their corresponding channel names',
     '8. What are the names of all the channels that have published videos in the year 2022',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names'))
if question == '1.What are the names of all the videos and their corresponding channels? ':
    query1 = "select videotitle,channel_name from videodetails;"
    mycursor.execute(query1)
    
    t1=mycursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
elif question=='2.Which channels have the most number of videos, and how many videos do they have ':
  query2="select ch_name, ch_v_count from channels order by ch_v_count desc;"
  mycursor.execute(query2)
    
  t2=mycursor.fetchall()
  st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. What are the top 10 most viewed videos and their respective channels':
    query3 = '''select videoVIEWcount, channel_name, videotitle from videodetails  order by videoVIEWcount desc limit 10;'''
    mycursor.execute(query3)
    
    t3 = mycursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))  

elif question == '4. How many comments were made on each video, and what are their corresponding video names':
    query4 = "select videocomments ,videotitle from videodetails ;"
    mycursor.execute(query4)
    
    t4=mycursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names':
    query5 = '''select videotitle, channel_name, videoLIKEcount from videodetails where videoLIKEcount is not null order by videoLIKEcount desc;'''
    mycursor.execute(query5)
    
    t5 = mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like Count"]))

elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names ':
    query6 = '''select videoLIKEcount, videotitle from videodetails;'''
    mycursor.execute(query6)
    
    t6 = mycursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))    

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names':
    query7 = "select ch_name, ch_view_count from channels;"
    mycursor.execute(query7)
    
    t7=mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022':
    query8 = '''select videotitle, videopublish, channel_name  from videodetails 
                where extract(year from videopublish) = 2022;'''
    mycursor.execute(query8)
    
    t8=mycursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names':
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


elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names':
    query10 = '''select videotitle, channel_name, videocomments  from videodetails 
                       where  videocomments is not null order by videocomments desc;'''
    mycursor.execute(query10)
    
    t10=mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
  
  



    









