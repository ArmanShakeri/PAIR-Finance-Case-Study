## Running the docker

- To get started run ``` docker-compose up ``` in root directory.
- It will create the PostgresSQL database and start generating the data.
- It will create an empty MySQL database.
- It will launch the analytics.py script. 

## Here are some points to consider:

- The insert part was not commented out in the faker code(main.py), so the records were not being saved in the table. This has been fixed now.
- I have tried to transfer the minimum amount of data to the processing node(analytics.py) and to perform as much aggregation and processing as possible on the database side to maintain data locality.
- Due to the increasing size of the input table, running queries will be time-consuming. A simple solution is to segment the execution of queries and fetching data from the database, for example, every five minutes, the last three hours of data are getting aggregated. Considering that the data is being received stream and duplicate data is not written to the output table due to the keys consideration(device_id,event_time), the metrics will be updated if new data arrives. Determining the interval depends on the business logics and needs. As this task is only for evaluation, there are no specific restrictions on reading from the database.
- The requested reports are saved in the form of a table named "report" in the "analytics" database in MySQL.