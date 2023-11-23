from googleapiclient.discovery import build
from isodate import parse_duration
import pandas as pd
from pymongo import MongoClient
import mysql.connector as sql
import pymysql 
import sqlalchemy
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
import re
import matplotlib.pyplot as plt
import plotly.express as px
from sqlalchemy import text


# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyClth7aj65yKLbY0AlSwuu9C7taV8vcTPA"  # "AIzaSyClth7aj65yKLbY0AlSwuu9C7taV8vcTPA"
youtube = build('youtube', 'v3', developerKey=api_key)

#******* Establishing connection with MySQL workbench *********
# CONNECTING WITH MYSQL DATABASE
 
user="root"
password="1234"
host="127.0.0.1"
database= "youtube"
port = "3306"

engine = create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
con = engine.connect()


#Function to get channel details:
def get_channel_details(channel_id):
    channel_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                       id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Channel_id = response['items'][i]['id'],
                    Channel_Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Channel_Subscribers_count = int(response['items'][i]['statistics']['subscriberCount']),
                    Channel_Views_count = int(response['items'][i]['statistics']['viewCount']),
                    Total_Videos_count = int(response['items'][i]['statistics']['videoCount']),
                    Description = response['items'][i]['snippet']['description'],
                    Channel_Published_Date = response['items'][i]['snippet']['publishedAt'],
                    Country = response['items'][i]['snippet']['country']
                    )
        channel_data.append(data)
    return channel_data

#Function to get playlist details:
def get_playlist(channel_id):
    playlist = []
    # Get the Uploads playlist ID for the specified channel
    uploads_playlist_id = get_uploads_playlist_id(channel_id)

    if uploads_playlist_id:
        response = youtube.playlistItems().list(part='snippet',
                                                playlistId=uploads_playlist_id,
                                                maxResults=25).execute()

        for i in range(len(response['items'])):
            play = dict(Channel_id=channel_id,
                        Playlist_id=response['items'][i]['snippet']['playlistId'],
                        Playlist_Title=response['items'][i]['snippet']['title'],
                        Playlist_count=0,  # You may need to modify this based on your specific use case
                        Playlist_Published_Date=response['items'][i]['snippet']['publishedAt'])
            playlist.append(play)

    return playlist

def get_uploads_playlist_id(channel_id):
    response = youtube.channels().list(part='contentDetails',
                                       id=channel_id).execute()

    if 'items' in response and response['items']:
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return uploads_playlist_id
    else:
        return None

# FUNCTION TO GET VIDEO IDS:
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        #check if there are more pages
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

#Function to convert duration to mins & seconds:
def parse_duration(video_duration):
    minutes_match = re.search(r'(\d+)M', video_duration)
    seconds_match = re.search(r'(\d+)S', video_duration)

    minutes = int(minutes_match.group(1)) if minutes_match else 0
    seconds = int(seconds_match.group(1)) if seconds_match else 0
    
    formatted_duration = f"{minutes}:{seconds:02}:00"
    return formatted_duration

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(c):
    video_stats = []
    for i in range(0, len(c)):
        response = youtube.videos().list(part="snippet,contentDetails,statistics", 
                                         id=', '.join(c[i:i + 50])).execute()
        
        for video in response['items']:
            video_duration = parse_duration(video['contentDetails']['duration'])  # Extract duration

            video_details = dict(
                Channel_name=video['snippet']['channelTitle'],
                Channel_id=video['snippet']['channelId'],
                Video_id=video['id'],
                Video_Title=video['snippet']['title'],
                Video_Thumbnail=video['snippet']['thumbnails']['default']['url'],
                Video_Description=video['snippet']['description'],
                Video_Published_Date=video['snippet']['publishedAt'],
                Video_Duration=video_duration,  # Include the duration
                Video_Views=int(video['statistics']['viewCount']),
                Video_Likes=int(video['statistics'].get('likeCount',0)),
                Video_Comments=int(video['statistics'].get('commentCount',0)),
                Video_Favorite_count=int(video['statistics']['favoriteCount']),
                Definition=video['contentDetails']['definition'],
                Caption_status=video['contentDetails']['caption'])
            video_stats.append(video_details)

    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(c):
    comment_data = []
    for i in c:
        try:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=i,
                                                    maxResults=10).execute()
            for cmt in response['items']:
                data = dict(Video_id = cmt['snippet']['videoId'],
                            Comment_id = cmt['id'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Comment_Like_count = int(cmt['snippet']['topLevelComment']['snippet']['likeCount']),
                            Comment_Reply_count = int(cmt['snippet']['totalReplyCount'])
                           )
                comment_data.append(data)
        except:
            pass
    return comment_data


#******* Establishing connection with MongoDB *********
Global_client = MongoClient("mongodb+srv://Santhosh_Rajendran:xPCuIlt071luW6GT@project007.omottlc.mongodb.net/?retryWrites=true&w=majority")
Database = Global_client["Youtube_data"]
my_collection = Database["Channels"]
channel_list = Database.list_collection_names()

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    channel_names_list = []
    for i in my_collection.find():
        channel_names_list.append(i['Channel_Details'][0]['Channel_name'])
    return channel_names_list

st.set_page_config(page_title='Youtube Data Harvesting and Warehousing with MongoDB, MySQL',page_icon = "▶️", layout="wide")

# Streamlit sidebar
with st.sidebar:
    selected_page = option_menu(
        menu_title="Features",
        options=["Home","---", "Extract", "Migrate", "Analysis Zone","About"],
        icons=["youtube","","cloud-arrow-up", "database-up", "clipboard2-data-fill","patch-question"],
        menu_icon="menu-up")
    
# Page contents
if selected_page == "Home":
    st.subheader("Hello Connections! Welcome to My Project Presentation")
    st.title("***Youtube Data Harvesting and Warehousing with 🍃MongoDB, 🧑‍💻MySQL***")
    tab1,tab2 = st.tabs(["Youtube Data Scrapping","  Applications and Libraries Used! "])
    with tab1:
        st.write(" Web scraping from YouTube using a scraper tool helps organizations gather valuable insights about video performance, user sentiment, and channel dynamics. By combining this data with information from social media, organizations can get a comprehensive view of their online presence and audience engagement. This approach enables data-driven decision-making and more effective content strategies.")
        st.write("[:open_book: Learn More  >](https://en.wikipedia.org/wiki/YouTube)")
        st.markdown("Benefits of youtube data harvesting")
        st.write(" =>  YouTube scraper simplifies the process of extracting valuable data from YouTube, making it quick, easy, and cost-effective. Whether you're a small business owner or part of a large organization's data department, using a scraper can offer numerous benefits for gathering insights from the platform.")
        st.markdown("Channel Data")
        st.write(""" =>  When scraping a YouTube channel page, you can extract data such as the number of subscribers, the total video count, playlists, and more. 
                         On the "About" page, you'll find information about the total number of views for the channel.""")
        st.markdown("Video Perfomance Data")
        st.write("When you scrape the page for a specific video, you’ll receive the number of views, likes, channel subscribers, comments, and more. While each of these metrics can be analyzed individually, it is important to keep the ration of the metrics in mind.")
        st.markdown("Sentimental Data")
        st.write("Fortunately, numerous dedicated fans, casual viewers, and new audiences often leave considerate comments, either in response to the video's content or about it. Consequently, your comment section essentially becomes a valuable source of data regarding fan requests or preferences for various types of videos. Scraping the comment section simplifies the process of keeping your subscribers satisfied and delivering content that aligns with their interests.")
    with tab2:
        st.write("  * Python")
        st.write("  * MySQL WorkBench")
        st.write("  * MongoDB")
        st.write("  * Streamlit")
#--------------------------------------------------------------------------------------------

elif selected_page == "About":
    st.header(" :blue[Project Conclusion]")
    tab1,tab2 = st.tabs(["Features","Connect with me on"])
    with tab1:
        st.write("The Streamlit application allows users to access and analyze data from multiple YouTube channels.")
        st.caption("This application have the following features :")
        st.write("1. Able to input a YouTube channel ID and retrieve all the relevant data (Channel name, Subscribers, Total video count, playlist ID, Video ID, Likes, Comments of each video) using Google API.")
        st.write("2. Option to store the data in a MongoDB database as a Data lake.")
        st.write("3. Able to collect data for up to 10 different YouTube channels and store them in the Data lake by clicking a upload button.")
        st.write("4. Option to select a channel name and migrate its data from the data lake to a MySQL database as Tables.")
        st.write("5. Able to search and retrieve data from the MySQL database using different search options, including joining tables to get channel details.")
    with tab2:
             # Create buttons to direct to different website
            linkedin_button = st.button("LinkedIn")
            if linkedin_button:
                st.write("[Redirect to LinkedIn Profile > (https://www.linkedin.com/in/santhosh-r-42220519b/)](https://www.linkedin.com/in/santhosh-r-42220519b/)")

            email_button = st.button("Email")
            if email_button:
                st.write("[Redirect to Gmail > santhoshsrajendran@gmail.com ](santhoshsrajendran@gmail.com)")

            github_button = st.button("GitHub")
            if github_button:
                st.write("[Redirect to Github Profile > https://github.com/Santhosh-1703 ](https://github.com/Santhosh-1703)")
#-------------------------------------------------------------------------------------------

elif selected_page == "Extract":
    st.header("Extraction of Youtube data using API Key & Loading to MongoDB Database")
    st.write("[👆🏽Click Here to YouTube >](https://www.youtube.com/)")
    st.info('Get Channel ID through clicking View page source📰', icon="ℹ️")
    channel_id = st.text_input("Enter the channel id :")

    if channel_id and st.button("Extract Data"):
        a = get_channel_details(channel_id)
        st.write(f'#### Extracted data from :green["{a[0]["Channel_name"]}"] channel')
        st.write(a)

    if st.button("Upload to MongoDB"):
        with st.spinner('Please wait for it.... '):
            def main(channel_id):
                a = get_channel_details(channel_id)
                b = get_playlist(channel_id)
                c = get_channel_videos(channel_id)
                d = get_video_details(c)
                e = get_comments_details(c)
                data = {"Channel_Details": a,
                        "Playlist_Details": b,
                        "Video_Details": d,
                        "Comments_Details": e}
                return data
            
            def insert_data_to_mongodb(channel_id):
                existing_data = my_collection.find_one({"Channel_Details.Channel_id": channel_id})
                if existing_data:
                    st.warning("Data for this channel already exists in MongoDB.")
                else:
                    data_to_insert = main(channel_id)
                    # Insert individual fields instead of the entire dictionary
                    my_collection.insert_one({
                        "Channel_Details": data_to_insert["Channel_Details"],
                        "Playlist_Details": data_to_insert["Playlist_Details"],
                        "Video_Details": data_to_insert["Video_Details"],
                        "Comments_Details": data_to_insert["Comments_Details"]})
                    st.success("Successfully Uploaded to MongoDB🍃Atlas Datalake!!!")
            insert_data_to_mongodb(channel_id)
#-------------------------------------------------------------------------------------------
 
elif selected_page == "Migrate":
    st.subheader('Channel data transformation for analysis')
    st.markdown("### Select a Channel to begin Transformation to MySQL")
    ch_names = channel_names()
    user_inp = st.selectbox("Select Channel", options=ch_names)

    Channel_data_details = [i for i in my_collection.find() if i["Channel_Details"][0]["Channel_name"] == user_inp]
    
    migrate = st.button("Migrate data to MySQL")

    # Check if the selected channel already exists in SQL
    selected_channel_id = Channel_data_details[0]["Channel_Details"][0]["Channel_id"]
    existing_data_sql = con.execute(text(
        "SELECT * FROM channel_data WHERE Channel_id = :channel_id LIMIT 1;"),
        {"channel_id": selected_channel_id}).fetchone()

    # If the channel already exists in SQL, show a warning
    if existing_data_sql:
        st.warning("Data for this channel already exists in MySQL.")
    else:
        if migrate:
            try:
                # Conversion of channel data to values
                Channel_Table = pd.DataFrame(Channel_data_details[0]["Channel_Details"])
                Channel_Table['Channel_Published_Date'] = pd.to_datetime(Channel_Table['Channel_Published_Date'])
                Channel_Table['Published_Date'] = pd.to_datetime(Channel_Table['Channel_Published_Date']).dt.date
                Channel_Table['Published_Time'] = pd.to_datetime(Channel_Table['Channel_Published_Date']).dt.time
                Channel_Table = Channel_Table.drop(columns=['Channel_Published_Date'])

                # Insert data into MySQL tables
                Channel_Table.to_sql("channel_data", con=engine, if_exists='append', index=False)

                # Check if the channel data transformation was successful before proceeding to other tables
                Playlist_Table = pd.DataFrame(Channel_data_details[0]["Playlist_Details"])
                Playlist_Table['Playlist_published_Date'] = pd.to_datetime(Playlist_Table['Playlist_Published_Date'])
                Playlist_Table['Published_Date'] = pd.to_datetime(Playlist_Table['Playlist_Published_Date']).dt.date
                Playlist_Table['Month'] = pd.to_datetime(Playlist_Table['Published_Date']).dt.month
                Playlist_Table['Year'] = pd.to_datetime(Playlist_Table['Published_Date']).dt.year
                Playlist_Table['Published_Time'] = pd.to_datetime(Playlist_Table['Playlist_Published_Date']).dt.time
                Playlist_Table = Playlist_Table.drop(columns=['Playlist_Published_Date'])

                Playlist_Table.to_sql("playlist_data", con=engine, if_exists='append', index=False)

                videos_Table = pd.DataFrame(Channel_data_details[0]["Video_Details"])
                videos_Table['Video_Published_Date'] = pd.to_datetime(videos_Table['Video_Published_Date'])
                videos_Table['Published_Date'] = pd.to_datetime(videos_Table['Video_Published_Date']).dt.date
                videos_Table['Month'] = pd.to_datetime(videos_Table['Published_Date']).dt.month
                videos_Table['Year'] = pd.to_datetime(videos_Table['Published_Date']).dt.year
                videos_Table['Published_Time'] = pd.to_datetime(videos_Table['Video_Published_Date']).dt.time
                videos_Table = videos_Table.drop(columns=['Video_Published_Date'])

                videos_Table.to_sql("video_data", con=engine, if_exists='append', index=False)

                Comments_Table = pd.DataFrame(Channel_data_details[0]["Comments_Details"])
                Comments_Table['Comment_posted_date'] = pd.to_datetime(Comments_Table['Comment_posted_date'])
                Comments_Table['Published_Date'] = pd.to_datetime(Comments_Table['Comment_posted_date']).dt.date
                Comments_Table['Month'] = pd.to_datetime(Comments_Table['Published_Date']).dt.month
                Comments_Table['Year'] = pd.to_datetime(Comments_Table['Published_Date']).dt.year
                Comments_Table['Publilshed_Time'] = pd.to_datetime(Comments_Table['Comment_posted_date']).dt.time
                Comments_Table = Comments_Table.drop(columns=['Comment_posted_date'])

                Comments_Table.to_sql("comment_data", con=engine, if_exists='append', index=False)

                # Display success message
                st.success("Transformation to MySQL Successful!")
            except Exception as e:
                # Display error message
                st.error(f"An error occurred: {e}")

#--------------------------------------------------------------------------------------------
elif selected_page == "Analysis Zone":
    st.write("## :orange[Select any Question to get Insights]")
    questions = st.selectbox('Questions',
                             ['Select','1.  What are the names of all the videos and their corresponding channels?',
                              '2.  Which channels have the most number of videos, and how many videos do they have?',
                              '3.  What are the top 10 most viewed videos and their respective channels?',
                              '4.  How many comments were made on each video, and what are their corresponding video names?',
                              '5.  Which videos have the highest number of likes and what are their corresponding channel names?',
                              '6.  What is the total number of likes for each video, and what are their corresponding video names?',
                              '7.  What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8.  What are the names of all the channels that have published videos in the year 2022?',
                              '9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10.  Which videos have the highest number of comments, and what are their corresponding channel names?'])
    st.write("#### :orange[Information Table]")
    
    if questions == 'Select':
        st.write("   ")

    elif questions == '1.  What are the names of all the videos and their corresponding channels?':
        Q1 = pd.read_sql("""SELECT Video_Title AS "Video Name", channel_name AS "Channel Name" FROM video_data 
                            ORDER BY Channel_name;""",con)
        Q1.index = Q1.index + 1
        st.write(Q1)

    elif questions == '2.  Which channels have the most number of videos, and how many videos do they have?':
        Q2 = pd.read_sql("""SELECT Channel_name AS "Channel Name", Total_videos_count AS "Total Videos"
                            FROM channel_data ORDER BY Total_videos_count DESC;""",con)
        Q2.index = Q2.index + 1
        tab1,tab2 = st.tabs(["Table","Chart"])
        with tab1:
            st.write(Q2)
        with tab2:
            fig = px.bar(Q2, x='Channel Name', y='Total Videos', text='Total Videos', 
                         title='Channels with the Most Videos', labels={'Total Videos': 'Video Count'}, 
                         color='Channel Name')
            st.plotly_chart(fig,use_container_width=True)

    elif questions == '3.  What are the top 10 most viewed videos and their respective channels?':
        Q3 = pd.read_sql("""SELECT Channel_name AS "Channel Name", Video_Title AS "Video Name",
                            Video_views AS "Video Views" FROM video_data 
                            ORDER BY Video_views DESC LIMIT 10;""",con)
        Q3.index = Q3.index + 1
        st.write(Q3)
    
    elif questions == '4.  How many comments were made on each video, and what are their corresponding video names?':
        Q4 = pd.read_sql("""SELECT Channel_name AS "Channel Name",Video_Title as "Video Title", 
                            Video_Comments AS "Total Comments"
                            FROM video_data
                            ORDER BY Video_Comments DESC;""",con)
        Q4.index = Q4.index + 1
        st.write(Q4)
    
    elif questions == '5.  Which videos have the highest number of likes and what are their corresponding channel names?':
        Q5 = pd.read_sql("""SELECT Channel_name AS "Channel Name",Video_Title AS "Video Title",Video_Likes AS "Like Count" 
                            FROM video_data
                            ORDER BY Video_Likes DESC;""",con)
        Q5.index = Q5.index + 1
        st.write(Q5)
    
    elif questions == '6.  What is the total number of likes for each video, and what are their corresponding video names?':
        Q6 = pd.read_sql("""SELECT Video_Title AS "Video Title",Channel_name as "Channel Name", Video_Likes AS "Like Count"
                            FROM video_data
                            ORDER BY Video_Likes DESC""",con)
        Q6.index = Q6.index + 1
        st.write(Q6)

    elif questions == '7.  What is the total number of views for each channel, and what are their corresponding channel names?':
        Q7 = pd.read_sql("""SELECT Channel_name AS "Channel Name", Channel_Views_count AS Views
                            FROM channel_data
                            ORDER BY Views DESC""",con)
        Q7.index = Q7.index + 1
        tab1,tab2 = st.tabs(["Table","Chart"])
        with tab1:
            st.write(Q7)
        with tab2:
            fig = px.bar(Q7, x='Views', y='Channel Name', text='Views',
                        title='Total Number of Views for Each Channel',
                        labels={'Views': 'Total Views'}, color='Channel Name')
            st.plotly_chart(fig,use_container_width=True)
    
    elif questions == '8.  What are the names of all the channels that have published videos in the year 2022?':
        Q8 = pd.read_sql("""SELECT Channel_name AS "Channel Name", COUNT(*) AS "Video Count"
                            FROM video_data WHERE YEAR = 2022
                            GROUP BY Channel_Name ORDER BY COUNT(*) DESC;""",con)
        Q8.index = Q8.index + 1
        st.write(Q8)
    
    elif questions == '9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        Q9 = pd.read_sql("""SELECT Channel_name AS "Channel Name",
                            ROUND(AVG(Video_duration), 2) AS "Average Video Duration"
                            FROM video_data GROUP BY Channel_name
                            ORDER BY AVG(Video_duration) DESC;""",con)
        Q9.index = Q9.index + 1
        tab1,tab2 = st.tabs(["Table","Chart"])
        with tab1:
            st.write(Q9)
        with tab2:
            fig = px.pie(Q9, names='Channel Name', values='Average Video Duration',
             title='Distribution of Average Video Durations in Each Channel',
             labels={'Average Video Duration': 'Average Duration'})
            # Display the Plotly pie chart using Streamlit
            st.plotly_chart(fig, use_container_width=True)

    elif questions == '10.  Which videos have the highest number of comments, and what are their corresponding channel names?':
        Q10 = pd.read_sql("""SELECT Channel_name AS "Channel Name",Video_Title as "Video Title", 
                             Video_Comments AS "Total Comments"
                             FROM video_data
                             ORDER BY Video_Comments DESC
                             LIMIT 10;""",con)
        Q10.index = Q10.index + 1
        st.write(Q10)