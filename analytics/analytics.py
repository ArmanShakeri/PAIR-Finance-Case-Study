from os import environ
from time import sleep
import json
from sqlalchemy import create_engine, text, Table, Column, Integer, String, TIMESTAMP, MetaData
from sqlalchemy.exc import OperationalError
import pandas as pd
from geopy import distance
import logging


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger=logging.getLogger(__name__) # this is python logger for show info and error in console
logger.setLevel(logging.DEBUG)
logger.info("Waiting for the data generator...")
sleep(20)
logger.info("ETL Starting...")

# check the connection to postgres
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)       
        break
    except OperationalError:
        logger.error("Connection to PostgresSQL unsuccessful.")
        sleep(0.1)
logger.info("Connection to PostgresSQL successful.")


def calculate_dist(locations):
    """This function is for calculating total distance for list of locations"""
    sum = 0
    for i in range(0,len(locations)-1):
        start_point = json.loads(locations[i])
        start_point = tuple(start_point.values())
        start_point = tuple(float(item) for item in start_point)

        end_point = json.loads(locations[i+1])
        end_point = tuple(end_point.values())
        end_point = tuple(float(item) for item in end_point)

        dist = distance.distance(start_point,end_point).km
        sum += dist
        
    return sum

def extract_locations(row):
    """This function is for extract list of location for every keys from dataframe"""
    sum = calculate_dist(row["locations"])
    return sum

def generate_report(psql_engine,statement):
    """This function is for fetch data from database"""
    with psql_engine.connect() as conn:
        result=conn.execute(text(statement))
    return result

def write_to_db(MYSQL_CS,df):
    """This function is for upsert(update and insert) records to database"""
    update_query = text("""
            INSERT INTO report (device_id, event_time, max_temp, total_count, distance)
            VALUES (:device_id, :event_time, :max_temp, :total_count, :distance)
            ON DUPLICATE KEY UPDATE
            max_temp = VALUES(max_temp),
            total_count = VALUES(total_count),
            distance = VALUES(distance)
        """)

    create_query = text("""
    CREATE TABLE IF NOT EXISTS `report` (
    `device_id` varchar(50),
    `event_time` timestamp,
    `max_temp` int ,
    `total_count` int,
    `distance` int ,
    UNIQUE KEY `idx_device_event` (`device_id`,`event_time`)
    )
    """)
    while True:
        try:
            mysql_engine = create_engine(MYSQL_CS, pool_pre_ping=True, pool_size=10)      
            break
        except OperationalError:
            logger.error("Connection to MySQL unsuccessful.")
            sleep(0.1)
    logger.info("Connection to MySQL successful.")
    
    with mysql_engine.connect() as conn:
        conn.execute(create_query)
        data = df.to_dict(orient="records")
        conn.execute(update_query, data)
        conn.commit()

def main():
    batch = 1 # variable for show how many batch is processed
    """The main function in which other functions are called"""
    report1="""
        SELECT device_id,date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)) as event_time,
        max(temperature) as max_temp
        from devices
        group by date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)),device_id
        order by device_id,date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp))
        """
    report2="""
        SELECT device_id,date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)) as event_time,
        count(1) as total_count from devices
        group by date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)),device_id
        order by device_id,date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp))
        """

    report3="""
        SELECT device_id,
        date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)) as event_time,
        array_agg("location") as locations
        from devices
        group by date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp)),device_id
        order by device_id,date_trunc('hour', cast(TO_TIMESTAMP(cast(time as int4)) as timestamp))
        """

    while True:
        try:
            result_report1 = generate_report(psql_engine, report1)
            result_report2 = generate_report(psql_engine, report2)
            result_report3 = generate_report(psql_engine, report3)

            df1 = pd.DataFrame(result_report1)
            df2 = pd.DataFrame(result_report2)
            df3 = pd.DataFrame(result_report3)
            df3["distance"] = df3.apply(extract_locations,axis=1)
            df3 = df3.drop(columns=["locations"])

            stage = pd.merge(df1, df2,how="outer",on=["device_id","event_time"])
            stage = pd.merge(stage, df3,how='outer',on=["device_id","event_time"])

            write_to_db(environ["MYSQL_CS"],stage)
            logger.info("batch: {}".format(batch))
        except Exception as e:
            logger.error(e.__str__())
        
        batch += 1  
        sleep(60) # generate report every minute

if __name__ == "__main__":
    main()

