import pandas as pd
import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load security key
load_dotenv()
API_KEY = os.getenv("YT_API_KEY")
CHANNEL_ID = "UCzwCEE_PchiBULMnAJqhGVg"

def fetch_safe_data():
    try:
        # Connect to YouTube
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        
        print("Connecting to YouTube API...")
        request = youtube.search().list(
            part="snippet",
            channelId=CHANNEL_ID,
            maxResults=50,
            type="video"
        )
        return request.execute()

    except HttpError as e:
        print(f"❌ API Error: You might have reached your daily limit or used a wrong key. Details: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return None

# Execution
response = fetch_safe_data()

if response:
    print("✅ Connection Successful! Processing data...")
    # Your transformation logic goes here
   
else:
    print("⚠️ Pipeline stopped because of an error.")
    # --- EXECUTE ---
response = fetch_safe_data()

if response:
    print("✅ Connection Successful! Processing data...")

    # 1. Convert the raw API response into a list
    video_list = []
    for item in response.get('items', []):
        video_list.append({
            'Title': item['snippet']['title'],
            'Published_At': item['snippet']['publishedAt'],
            'Video_ID': item['id']['videoId']
        })
    # 2. Create the Table (DataFrame)
    df = pd.DataFrame(video_list)
    # 3. Print the Result
    if not df.empty:
        print("\n--- Your YouTube Data ---")
        print(df) # This line prints the table to your terminal
        
        # 4. Save to CSV
        df.to_csv("youtube_data_secure.csv", index=False)
        print("\n✅ Saved to youtube_data_secure.csv")
    else:
        print("⚠️ Connection worked, but no videos were found for this Channel ID.")
else:
    print("⚠️ Pipeline stopped because of an error.")
# 1. Convert the 'Published_At' to a standard Date format (YYYY-MM-DD)
df['Published_At'] = pd.to_datetime(df['Published_At']).dt.strftime('%Y-%m-%d')
df['Title'] = df['Title'].str.title()

# 3. Add a "Serial Number" column just like your marksheet
df.insert(0, 'Sr_No', range(1, 1 + len(df)))
print("\n--- Final Cleaned Data (Ready for Business) ---")
print(df[['Sr_No', 'Title', 'Published_At']].head())

# Save the absolute final version
df.to_csv("youtube_final_report.csv", index=False)
import sqlite3
conn = sqlite3.connect('youtube_database.db')
df.to_sql('trending_videos', conn, if_exists='replace', index=False)
print("\n✅ Success! Your data is now inside a SQL Database (youtube_database.db)")
query = "SELECT Title, Views FROM trending_videos WHERE Views > 1000"
high_view_videos = pd.read_sql(query, conn)
print("\n--- Results from SQL Query ---")
print(high_view_videos)
conn.close()