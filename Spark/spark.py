import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fx
import pandas as pd

manualTest =  False
csv_path = './raw_data/booking.csv' # set file to csv to work with
save_csv = './Spark/TopRoom_type.csv'  # set location to save final csv file

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName('SparkTransform') \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# fetch data and render to spark dataframe
df = spark.read.csv(csv_path,
                     header="true")

# get the top 3 most booked room_type
top_room = df.groupby('room_type').agg(fx.count('room_type'))\
    .orderBy('count(room_type)', ascending=False).limit(5)

# save the variables to dict
TopRoom_type = {}
TopRoom_type['most_booked_room'] = [row['room_type'] for row in top_room.collect()]

# get the top 5 currency for the most booked room
top_currency = df.groupby('currency').agg(fx.count('room_type')) \
    .orderBy('count(room_type)', ascending=False).limit(5)

# get a list with only the currency name
TopCurrency_name = [row['currency'] for row in top_currency.collect()]
for item in TopCurrency_name:
    currency_df = df.where(df.currency==item)\
        .groupBy('room_type').agg(fx.count('room_type'))\
        .orderBy('count(room_type)', ascending=False).limit(5)
        # save the top room_type for each hotel_id
    TopRoom_type[item] = [row['room_type'] for row in currency_df.collect()]

# transform the data into a Pandas dataframe
final_df = pd.DataFrame.from_dict(TopRoom_type, orient='index')
final_df = final_df.transpose()
# save the data to a csv
final_df.to_csv(save_csv, encoding='utf8', index=False) 