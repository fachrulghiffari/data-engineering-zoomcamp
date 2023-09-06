
# Import libraries
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

# Create a function to ingest data from the web to PostgreSQL
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name[0]
    table_name1 = params.table_name[1]
    url = params.url[0]
    url1 = params.url[1]

    # Create an SQLAlchemy engine object to establish a connection with a PostgreSQL database.
    # The create_engine() function takes a database URL as its argument,
    # providing information about the database type (PostgreSQL), host, port,
    # database name, and authentication details if necessary
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read data from a CSV file located at the specified URL in an iterator mode
    # with each iteration reading a chunk of 100,000 rows at a time.
    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)

    # Retrieve the first "chunk" of data from the iterator df_iter.
    df=next(df_iter)

    # Record the current time.
    # start_time = the start time of the transform and data load process for one chunk.
    start_time = time()

    # Convert the 'tpep_pickup_datetime' and 'tpep_dropoff_datetime' columns to datetime objects.
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create a table in postgreSQL and load columns names
    # 'if_exists=replace' is an argumen that will replace table with the same name.
    # Index column will not be included in the table.
    df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
    
    # Insert rows of data into the database
    df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
    count_rows = len(df)

    # Record the current time
    # end_time = the end time of the transform and data load process for one chunk
    end_time = time()

    print(f'Inserted {len(df)} rows. Total rows: {count_rows}. Time Taken {round(end_time-start_time,2)}')
    
    # total_time variable is time to used to run the process of transform and load data
    total_time = round(end_time-start_time, 2)

    # Transform & load data to database at each iteration df_iter
    for df in df_iter:
    
        start_time = time()
        
       
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

        count_rows += len(df)

        end_time = time()

        print(f'Inserted {len(df)} rows to {table_name}. Total rows: {count_rows}. Time Taken {round(end_time-start_time,2)}s')

        total_time += round(end_time-start_time,2)

    print(f"\nTotal rows inserted to {table_name} table: {count_rows} rows\nTotal time taken: {round(total_time,2)}s")

    # The codes below are the process of transforming and loading taxi_zones data
    start_time = time()

    taxi_zone = pd.read_csv(url1)
    taxi_zone.to_sql(name=table_name1, con=engine, index=False, if_exists='replace')
    zone_count_rows = len(taxi_zone)

    end_time = time()

    print(f"\nTotal rows inserted to {table_name1} table: {zone_count_rows} rows\nTotal time taken: {round(end_time-start_time,2)}s")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='username for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', nargs=2, help='name of the table where we will write the result for')
    parser.add_argument('--url', nargs=2, help='source url')
    

    args = parser.parse_args()

    main(args)