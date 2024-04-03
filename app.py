from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import last, avg, stddev, col, lag, concat, lit
from math import sqrt


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    FloatType,
    LongType,
)

def get_data() -> DataFrame:
    # Create a Spark session
    spark = SparkSession.builder \
            .appName("Vi-home-assignment") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
    
    # define schema
    schema = StructType(
        [
            StructField("date", DateType(), False),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", LongType(), True),
            StructField("ticker", StringType(), False),
        ]
    )

    # load data from csv
    source_path = "s3://aws-glue-home-assignment-yv/data/stock_prices.csv"

    df = spark.read.csv(source_path, dateFormat="MM/d/yyyy", header=True, schema=schema)

    # sort by date
    window = Window.partitionBy("ticker").orderBy("date")

    # fill close price nulls by using closest non-null values
    df = df.withColumn(
        "close", last("close", ignorenulls=True).over(window)
    )

    # calculate daily returns by using the percentage difference between two consecutive days' closing prices
    returns = (
        (col("close") - lag("close", 1).over(window)) / lag("close", 1).over(window)
    ) 
    df = df.withColumn("returns", returns)

    return df

def save_to_csv(df, filename):
    dest_path = f"s3://aws-glue-home-assignment-yv/output/{filename}"
    df.write.mode("overwrite").csv(dest_path)
    print(f"Saved to {dest_path}")

def q1_avg_daily_return(df):
    # Group by date and calculate the average return
    # sort by date
    avg_return = df.groupBy("date").avg("returns").orderBy("date")

    # save to csv
    save_to_csv(avg_return, "q1_avg_return")

def q2_most_traded_stock(df, topn:int= 1):
    # Calculate the traded volume by multiplying the closing price by the volume
    df = df.withColumn("frequency", df["close"] * df["volume"])
    
    # Group by ticker and calculate the average traded volume, sort by average traded volume descending
    average_frequency = df.groupBy("ticker").agg(avg("frequency").alias("frequency"))
    # get the most traded stock (highest average traded volume)
    average_frequency = average_frequency.orderBy("frequency", ascending=False).limit(topn)
    
    # save to csv
    save_to_csv(average_frequency, "q2_avg_frequency")


def q3_annualized_std(df, topn:int= 1):
    # Group by ticker and calculate the standard deviation of the returns
    # Annualized standard deviation = Standard Deviation * SQRT(N) where N = number of periods in 1 year.
    # sort by values
    N = 52 * 5 # 52 weeks in a year, 5 trading days in a week
    annualized_std = df.groupBy("ticker").agg((stddev("returns") * sqrt(N)).alias("annualized_std_returns")).orderBy("annualized_std_returns", ascending=False).limit(topn)

    # save to csv
    save_to_csv(annualized_std, "q3_annualized_std")

def q4_top_n_range_day_return_dates(df, lag_days:int= 30, topn:int= 3):
    # Calculate the 30-day return by joining the dataframe with itself on the date 30 days prior
    window = Window.partitionBy('ticker').orderBy('date')
    # df = df.withColumn("30_day_prior_close", last("close", ignorenulls=True).over(window.rowsBetween(-30, -30)))
    df = df.withColumn("previous_close", lag("close", lag_days).over(window))
    # concatenate date and ticker into new column ticker_date
    df = df.withColumn("ticker_date", concat(col("ticker"), lit("_"), col("date")))
    # calculate the change in return rate within range
    df = df.withColumn("return_rate_change_within_range", (df["close"] - df["previous_close"]) / df["previous_close"]).orderBy("return_rate_change_within_range", ascending=False).limit(topn)
    
    # save to csv
    save_to_csv(df[["ticker_date", "return_rate_change_within_range"]], "q4_top_days_range_return_dates")

def main(): 
    df = get_data()

    q1_avg_daily_return(df)
    q2_most_traded_stock(df, topn=1)
    q3_annualized_std(df, topn=1)
    q4_top_n_range_day_return_dates(df, lag_days=30, topn=3)

if __name__ == "__main__":
    main()
