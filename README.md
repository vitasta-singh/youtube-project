# youtube-data harvesting and warehousing using SQL mongodb and streamlit
LIBRARIES USED  

1.googleapiclient.discovery  

2.streamlit 

3.MYSQL

4.PYMONGO

5.PANDAS

1)STREAMLIT  
Streamlit is an open-source Python framework for machine learning and data science teams. From streamlit we can enter our channel id and observe the changes and table of required field
PYTHON  
Python is a popular programming language.Python can be used on a server to create web applications.
SQL  
SQL is a standard language for storing, manipulating and retrieving data in databases.
MONGODB  
Records in a MongoDB database are called documents, and the field values may include numbers, strings, booleans, arrays, or even nested documents.
PREOCEDURE OF THIS PROJECT  
From youtube one should create their API KEY. Using that api key one can easily fetch data from youtube resources and observe the channel details,video ids from that video ids one can collect the video details of each videos. The similar process is carried for comments. After collecting all this data we transfer this data to mongo db after creating connection using mongoclient. From mongoclient we establish a connection with MYSQL and create tables of channels video comments and transfered those collected mongodata in SQL tables.
Now the major work begins with streamlite in streamlit we create a beautiful page and there we used the user given channel id to collect the data from youtube and storing it to mongo.  Now that store data is used by SQL by migrating it to SQL. Then we use this table to answer our queries.
  
  REFRENCES:
https://www.w3schools.com/
https://streamlit.io/


