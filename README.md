**YOUTUBE DATA HARVESTING AND WAREHOUSING**
      
  YouTube Data Harvesting and Warehousing is a project designed to provide users with the ability to access and analyze data from numerous YouTube channels. Utilizing SQL, MongoDB, and Streamlit, the project delivers a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

  The system is capable of harvesting and managing YouTube channel data, including channel details, video information, playlists, and comments. Data is collected using the YouTube Data API and stored in a MongoDB database. The application also supports migrating the data to a PostgreSQL database and offers various insights and queries through a Streamlit web interface.

**Features**
     **Collect and Store Data**
            Fetch and store details of a YouTube channel, including channel information, playlists, videos, and comments in MongoDB.
      **Migrate to SQL**
            Migrate the collected data from MongoDB to a PostgreSQL database.
      **Show Channels**
            Display all channel details stored in MongoDB.
     **Show Playlists**
            Display all playlist details stored in MongoDB.
      **Show Videos**
            Display all video details stored in MongoDB.
      **Show Comments**
            Display all comment details stored in MongoDB.
      **Question and Answer**
            Answer specific queries related to the YouTube data, such as the most viewed videos, channels with the most videos, and average video duration.
**Libraries Used**
    Python
    Streamlit
    MongoDB
    PostgreSQL
    pandas
**Usage**
**Streamlit Web Interface**
      The Streamlit application provides an interface to visualize and query the YouTube data. Use the application to:
          View details of YouTube channels.
          Explore playlists and videos of a channel.
          Analyze comments on videos.
          Perform various queries to gain insights, such as identifying the most viewed videos or channels with the most content.
