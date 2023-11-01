# youtube-data-Harvesting-and-Warehousing-

**Introduction:**

This project involves creating a user-friendly Streamlit application that utilizes the Google API to collect insightful data from YouTube channels. The acquired data is stored in MongoDB, then transitioned to a SQL data warehouse for analysis and exploration, all accessible through the Streamlit app.

**Technologies Used:**

* Python scripting
* Data Collection
* API integration
* Streamlit
* Data Management using MongoDB (Atlas) and SQL
* Plotly

**Installation**

To run this project, you need to install the following packages:
   
    pip install google-api-python-client
    pip install pymongo
    pip install pandas
    pip install pymysql
    pip install streamlit
    pip install plotly

**Features**

* Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
* Store the retrieved data in a MongoDB database.
* Option to check wheather the respective channel Data is exist or Not in MongoDb (Atlas).
* Migrate the data to a MySQL data warehouse.
* Analyze and visualize data using Streamlit and Plotly.
* Perform queries on the MySQL data warehouse.
* Display the list of channel name's along with subcription & view's count.

**Retrieving data from the YouTube API**

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments.

**Storing data in MongoDB**

The retrieved data is stored in a MongoDB database based on channel Id.
Before storing the data in mongodb we used the one of the Option to check wheather the respective channel Data is exist or Not in MongoDb (Atlas).

**Migrating data to a MySQL data warehouse**

The application allows users to migrate data from MongoDB to a MySQL data warehouse. Users can given the input as channel id to migrate the data to Mysql database. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing MySQL queries.

**Analysis**

The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.

It highlights how Plotly enables the creation of a variety of charts to enhance data understanding and pattern recognition. The Streamlit app's user-friendly interface allows for interactive exploration and customization of these visualizations, offering users the tools to uncover valuable insights and make informed, data-driven decisions.

**Conclusion**

In summary, this project leverages the Google API to collect, store, and analyze data from YouTube channels, making it accessible through a user-friendly Streamlit application. With MongoDB for initial storage and MySQL for structured data warehousing, users can seamlessly transition and explore data. The integration of Plotly empowers users to create insightful visualizations, enhancing data understanding and enabling data-driven decision-making. This comprehensive approach streamlines the entire process, making it a valuable tool for data enthusiasts and analysts.

**User Interface**


![image](https://github.com/DineshDhamodharan24/youtube-data-Harvesting-and-Warehousing-/assets/142207421/a03178ca-2b66-40f6-bf41-db9acfe05b38)




