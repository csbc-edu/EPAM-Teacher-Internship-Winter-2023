from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 2 Load data to DataFrames or DataSets
spark = SparkSession.builder.appName("Theme08").getOrCreate()
df = spark.read.csv("data/2007.csv", header=True)
main_df = df    # to have
df_carriers = spark.read.csv("data/carriers.csv", header=True)
df_airports = spark.read.csv("data/airports.csv", header=True)

# 3 Count total number of flights per carrier in 2007
total_number_of_flights_per_carrier = df.where('Year == 2007').groupBy("UniqueCarrier").count()
total_number_of_flights_per_carrier.show()

# 4 The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports data)
df_jun_2007 = main_df.filter((df.Month == 6) & (df.Year == 2007))['Year', 'Month', 'Origin']

number_of_flights = df_jun_2007.join(df_airports, df_jun_2007['Origin'] == df_airports['iata'])\
    .select('airport', 'state')\
    .where('state == "NY"').count()

print(f"In Jun 2007 we had served {number_of_flights} flights in NY\n") # because of arrival and departure registration maybe we need to divide by 2

# 5  Find five most busy airports in US during Jun 01 - Aug 31.
df_summer_2007 = main_df.filter((df.Month >= 6) & (df.Month <= 8)).groupBy('Origin').count()
top5_most_busy_airports = df_summer_2007.orderBy(col('count').desc()).limit(5)
top5_most_busy_airports.join(df_airports, top5_most_busy_airports['Origin'] == df_airports['iata'])\
    .select('airport', top5_most_busy_airports['count']).orderBy(col('count').desc()).show() # maybe, better to have a custom method for join with airports' names

# 6 Find the carrier who served the biggest number of flights.
carrier_served_biggest_number_of_flights = total_number_of_flights_per_carrier.orderBy(col("count").desc())
df_carriers.where(df_carriers['Code'] == carrier_served_biggest_number_of_flights.collect()[0]['UniqueCarrier']).show()

