# Real_time_Crypto_data_analytics_GCP
<h1>Project Overview </h1>
<h3>This project aims to create a comprehensive data analysis pipeline for processing and analyzing real-time cryptocurrency market data from the Binance API. The pipeline covers data collection, real-time streaming, ETL (Extract, Transform, Load) processing, data storage, SQL transformations, and data visualization.</h3>
<h2> Project Architecture </h2>
<a target="_blank" href="https://imageupload.io/ROXMOmQrF6LFouu"><img  src="https://imageupload.io/ib/zwiZmT4pmEBjnuF_1696522451.jpeg" alt="Blank diagram.jpeg"/></a>
<h3>Data Collection from Binance API</h3>


Retrieve historical data from the Binance API using Python and store it in a suitable data structure

**Data Collection**: The pipeline begins by fetching real-time cryptocurrency market data from the Binance API. This includes information such as price, volume, and order book data for various cryptocurrencies.

**Real-Time Streaming**: We leverage streaming technologies to ensure that our pipeline can handle continuous data updates. Real-time streaming ensures that our analysis remains up-to-date with the dynamic cryptocurrency market.

**ETL Processing (Extract, Transform, Load)**: Data collected from the Binance API often requires cleaning, transformation, and enrichment to be useful for analysis. Our ETL process handles these tasks efficiently, making sure your data is ready for analysis.

**Data Storage**: Processed data is securely stored in a data repository, ensuring data integrity and easy access for future analysis. You can choose from a variety of storage solutions, depending on your requirements.

**SQL Transformations**: SQL-based transformations can be applied to the stored data to generate meaningful insights and reports. You can perform complex queries and aggregations to gain deeper insights into the cryptocurrency market.



<h4> Creation of a topic in Cloud Pub/Sub </h4>
<a target="_blank" href="https://imageupload.io/1d6wXvDViFRq4rj"><img  src="https://imageupload.io/ib/T27sAkCmxdyFBBe_1696593521.jpg" alt="Cloud Pub-Sub.JPG"/></a> 

<h4> Data generated from Binance API is sended in a bucket name 'ayoubcryptodata' </h4>
<a target="_blank" href="https://imageupload.io/eshj3qV6jR8525k"><img  src="https://imageupload.io/ib/C3ABvRgYlNeRM6J_1696601032.jpg" alt="GCS1.JPG"/></a>

<a target="_blank" href="https://imageupload.io/u4xMZ4ZX2FI4acX"><img  src="https://imageupload.io/ib/M4frD0lpOCS45an_1696601426.jpg" alt="uploa.JPG"/></a>

<h4> Data Transformations in BigQuery </h4>
<a target="_blank" href="https://imageupload.io/K53MFp61V85FMmX"><img  src="https://imageupload.io/ib/aabD5dWsrAfuhyp_1696601579.jpg" alt="Crypto-Table.JPG"/></a>

<h4> Data visualization in PowerBI</h4>
<a target="_blank" href="https://imageupload.io/5MJp8lJqA4UtcRh"><img  src="https://imageupload.io/ib/NwKQKNw8diPeinr_1696607225.jpg" alt="Dashboard Crypto.JPG"/></a>
