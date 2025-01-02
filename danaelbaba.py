# -*- coding: utf-8 -*-
"""DanaELBABA.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1cC4ecw3xqjWmktectK-wesMhwjIdAaiZ

### Fetch stock data using yfinance.
"""

import yfinance as yf

stocks = ["AAPL", "MSFT", "GOOGL", "TSLA"]

# Fetch data for each stock and save as CSV
for stock in stocks:
    data = yf.download(stock, start="2020-01-01", end="2023-12-31")
    data.reset_index(inplace=True)
    data.to_csv(f"{stock}.csv", index=False)

from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, mean, min, corr, max, to_date, lag, lit
from pyspark.sql.types import *

spark_application_name = "Stock Analysis"
spark = (SparkSession.builder.appName(spark_application_name).getOrCreate())

"""Read the CSV files into Spark DataFrames and combine them."""

schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Open", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Stock", StringType(), False)
])

dfs = []
for stock in stocks:
    # Read CSV with schema enforcement
    df = spark.read.csv(f"{stock}.csv", header=True, schema=schema)

    # Add a Stock column with the symbol
    df = df.withColumn("Stock", lit(stock))
    dfs.append(df)

# Combine all stocks into a single DataFrame
combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

# Print the schema and preview the DataFrame
combined_df.printSchema()
combined_df.show(5)

# Check for null or invalid values
combined_df.select([col(c).isNull().alias(f"null_{c}") for c in combined_df.columns]).show()

# Confirm column data types
combined_df.printSchema()

"""### Pre-process the Data

Ensure the Date column is in the correct format.
"""

combined_df = combined_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

"""Drop rows with missing values."""

combined_df = combined_df.dropna()

"""Add Calculated Columns"""

window_spec = Window.partitionBy("Stock").orderBy("Date")

combined_df = combined_df.withColumn(
    "Prev_Close", lag("Close").over(window_spec)
).withColumn(
    "Daily_Return", (col("Close") - col("Prev_Close")) / col("Prev_Close")
)

combined_df.show(5)

"""Calculate a 5-day moving average for closing prices."""

combined_df = combined_df.withColumn(
    "Moving_Avg_5", mean("Close").over(window_spec.rowsBetween(-4, 0))
)
combined_df.show(5)

"""## Aggregation and Analysis

Calculate total, average, minimum, and maximum closing prices for each stock.
"""

summary_stats = combined_df.groupBy("Stock").agg(
    sum("Close").alias("Total_Close"),
    mean("Close").alias("Avg_Close"),
    min("Close").alias("Min_Close"),
    max("Close").alias("Max_Close")
)
summary_stats.show()

"""Find the stock with the highest daily return."""

highest_return = combined_df.orderBy(col("Daily_Return").desc()).select("Stock", "Date", "Daily_Return").first()
print(f"Highest daily return: {highest_return}")

combined_df.write.csv("processed_stocks1.csv", header=True)

""" Show the First and Last 40 Rows"""

combined_df.show(40)
combined_df.orderBy(col("Date").desc()).limit(40).show(40, truncate=False)

"""Get the Number of Observations"""

num_observations = combined_df.count()
print(f"Number of observations: {num_observations}")

"""Deduce the Period Between Data Points"""

from pyspark.sql.functions import datediff

def deduce_period(df):
    # Ensure the DataFrame is sorted by Date
    df = df.orderBy("Date")

    # Calculate difference between consecutive dates
    window_spec = Window.orderBy("Date")
    df = df.withColumn("Prev_Date", lag("Date").over(window_spec))
    df = df.withColumn("Date_Diff", datediff(col("Date"), col("Prev_Date")))

    # Determine the most common period
    period = df.groupBy("Date_Diff").count().orderBy(col("count").desc()).first()
    print(f"Most common period: {period['Date_Diff']} days")
    return period['Date_Diff']
deduce_period(combined_df)

"""Descriptive Statistics"""

from pyspark.sql.functions import mean, stddev

def descriptive_statistics(df):
    stats = df.describe()
    print("Descriptive Statistics:")
    stats.show()
descriptive_statistics(combined_df)

from pyspark.sql.functions import col, sum

def count_missing_values(df):
    missing_counts = df.select(
        *[(sum(col(c).isNull().cast("int")).alias(f"missing_{c}")) for c in df.columns]
    )
    print("Missing Values:")
    missing_counts.show()
count_missing_values(combined_df)

"""Correlation Between Values"""

from pyspark.sql.functions import col, sum, corr

def calculate_correlation_matrix(df):
    # Get numeric columns
    numeric_cols = [col_name for col_name, dtype in df.dtypes if dtype in ("int", "double")]

    if not numeric_cols:
        print("No numeric columns available for correlation.")
        return

    correlation_matrix = {}

    print("Correlation Matrix:")
    for i, col1 in enumerate(numeric_cols):
        for col2 in numeric_cols[i:]:  # Avoid duplicate pairs
            if col1 == col2:
                correlation_value = 1.0
            else:
                correlation_value = df.stat.corr(col1, col2)

            correlation_matrix[(col1, col2)] = correlation_value
            correlation_matrix[(col2, col1)] = correlation_value

    # Print the matrix
    print(f"{'':<15}", end="")
    for col in numeric_cols:
        print(f"{col:<15}", end="")
    print()
    for col1 in numeric_cols:
        print(f"{col1:<15}", end="")
        for col2 in numeric_cols:
            print(f"{correlation_matrix[(col1, col2)]:<15.2f}", end="")
        print()

calculate_correlation_matrix(combined_df)

"""Average of Opening and Closing Prices for Each Stock (Week, Month, Year)"""

def calculate_average_prices_for_stock(df, stock=None):
    from pyspark.sql.functions import year, month, weekofyear, avg

    df = df.withColumn("Year", year("Date")) \
           .withColumn("Month", month("Date")) \
           .withColumn("Week", weekofyear("Date"))

    if stock:
        df = df.filter(df["Stock"] == stock)

    avg_prices = df.groupBy("Stock", "Year", "Month", "Week") \
                   .agg(avg("Open").alias("Avg_Open"), avg("Close").alias("Avg_Close"))

    avg_prices.orderBy("Stock", "Year", "Month", "Week").show(50)  # Adjust number of rows displayed as needed
    return avg_prices

calculate_average_prices_for_stock(combined_df, stock="AAPL")
calculate_average_prices_for_stock(combined_df, stock="MSFT")
calculate_average_prices_for_stock(combined_df, stock="GOOGL")
calculate_average_prices_for_stock(combined_df, stock="TSLA")

"""Daily and Monthly Changes in Stock Prices

"""

combined_df.select("Stock").distinct().show()

def calculate_daily_return(df):
    df = df.withColumn("Daily_Return", (col("Close") - lag("Close").over(window_spec)) / lag("Close").over(window_spec))

    df.show(10)
    return df
calculate_daily_return(combined_df)

"""To find the stocks with the highest daily return we do the following:"""

from pyspark.sql.functions import col, max

def get_stock_with_highest_daily_return(df):
    # Find the maximum daily return for each stock
    stock_max_return = df.groupBy("Stock").agg(max("Daily_Return").alias("Max_Daily_Return"))

    # Find the stock with the overall highest daily return
    highest_return = stock_max_return.orderBy(col("Max_Daily_Return").desc()).limit(1)

    # Show the results
    stock_max_return.show()  # Max daily return per stock
    highest_return.show()    # Stock with the highest daily return overall

    return stock_max_return, highest_return

stock_max_return, highest_return = get_stock_with_highest_daily_return(combined_df)

"""Calculate the average daily return for different periods like week, month, and year"""

from pyspark.sql.functions import year, month, weekofyear, avg

def calculate_average_daily_return(df):

    # Extract year, month, and week information
    df = df.withColumn("Year", year("Date")) \
           .withColumn("Month", month("Date")) \
           .withColumn("Week", weekofyear("Date"))

    # Calculate average daily return per week
    weekly_avg_return = df.groupBy("Stock", "Year", "Week") \
                          .agg(avg("Daily_Return").alias("Avg_Daily_Return_Weekly"))

    # Calculate average daily return per month
    monthly_avg_return = df.groupBy("Stock", "Year", "Month") \
                           .agg(avg("Daily_Return").alias("Avg_Daily_Return_Monthly"))

    # Calculate average daily return per year
    yearly_avg_return = df.groupBy("Stock", "Year") \
                          .agg(avg("Daily_Return").alias("Avg_Daily_Return_Yearly"))

    # Display the results
    print("Weekly Average Daily Return:")
    weekly_avg_return.orderBy("Stock", "Year", "Week").show(10)

    print("Monthly Average Daily Return:")
    monthly_avg_return.orderBy("Stock", "Year", "Month").show(10)

    print("Yearly Average Daily Return:")
    yearly_avg_return.orderBy("Stock", "Year").show(10)

    return weekly_avg_return, monthly_avg_return, yearly_avg_return

weekly_avg, monthly_avg, yearly_avg = calculate_average_daily_return(combined_df)

"""Yearly Returns: The yearly average daily returns show that most stocks maintained relatively stable performance, with values typically close to zero. However, certain years (AAPL in 2022 with -9.92) exhibit significant negative performance, potentially indicating a challenging year for that stock.

Monthly and Weekly Volatility: The monthly and weekly average daily returns for AAPL show fluctuations, with some months (May 2020 and July 2020) having positive returns, while others (February 2020 and September 2020) show negative returns. These variations reflect the inherent volatility of stock prices over shorter periods.

Calculate the moving average for a specified column and period.
    
    :param df: PySpark DataFrame
    :param column_name: The name of the column to calculate the moving average for
    :param num_points: The number of periods to consider for the moving average
    :return: DataFrame with a new column containing the moving average
"""

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

def calculate_moving_average(df, column_name, num_points):

    # Define a window for calculating the moving average
    window_spec = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-num_points + 1, 0)

    # Calculate the moving average
    moving_avg_column = f"Moving_Avg_{column_name}_{num_points}"
    df = df.withColumn(moving_avg_column, avg(col(column_name)).over(window_spec))

    # Show a sample of the resulting DataFrame
    df.show(10)

    return df

# Calculate the 5-period moving average for the 'Open' column
combined_df_with_moving_avg = calculate_moving_average(combined_df, "Open", 5)

"""Analyze the Results Across All Stocks:"""

from pyspark.sql.functions import avg

avg_moving_avg = combined_df_with_moving_avg.groupBy("Stock").agg(
    avg("Moving_Avg_Open_5").alias("Avg_Moving_Avg_Open_5")
)
avg_moving_avg.show()

"""We can see that MSFT has the highest average 5-period moving average of opening prices (262.24), indicating consistently higher opening prices compared to other stocks.
GOOGL has the lowest average moving average (107.65), suggesting it has lower overall opening prices in comparison.
Finally, TSLA and AAPL are in the middle range, with TSLA showing slightly higher average opening prices than AAPL.
This indicates varying price levels and trends among the stocks over time.

Calculate the correlation between two stocks based on a specified column.
    
    :param df: PySpark DataFrame containing stock data
    :param stock1: The first stock symbol (e.g., "AAPL")
    :param stock2: The second stock symbol (e.g., "MSFT")
    :param column: The column to calculate correlation on
    :return: Correlation value between the two stocks
"""

def calculate_correlation_between_stocks(df, stock1, stock2, column="Close"):

    # Filter data for the two stocks
    stock1_data = df.filter(col("Stock") == stock1).select("Date", col(column).alias(f"{column}_{stock1}"))
    stock2_data = df.filter(col("Stock") == stock2).select("Date", col(column).alias(f"{column}_{stock2}"))

    # Join the two stocks' data on Date
    joined_data = stock1_data.join(stock2_data, on="Date", how="inner")

    # Calculate the correlation
    correlation = joined_data.stat.corr(f"{column}_{stock1}", f"{column}_{stock2}")

    print(f"Correlation between {stock1} and {stock2} based on {column}: {correlation}")
    return correlation

calculate_correlation_between_stocks(combined_df, "AAPL", "MSFT", column="Close")
calculate_correlation_between_stocks(combined_df, "AAPL", "GOOGL", column="Close")
calculate_correlation_between_stocks(combined_df, "AAPL", "TSLA", column="Close")
calculate_correlation_between_stocks(combined_df, "MSFT", "GOOGL", column="Close")
calculate_correlation_between_stocks(combined_df, "MSFT", "TSLA", column="Close")
calculate_correlation_between_stocks(combined_df, "GOOGL", "TSLA", column="Close")

"""We notice that strongest correlation: AAPL and MSFT (0.93) show the highest correlation, reflecting similar market trends in the tech sector.

Moderate correlation: AAPL and GOOGL (0.83) and GOOGL and TSLA (0.84) indicate moderately similar price movements.

Weakest correlation: MSFT and TSLA (0.76) and AAPL and TSLA (0.79) suggest TSLA is less influenced by tech market trends.

Calculate return rate from grouped data that includes first and last close prices.
    
    :param df_grouped: PySpark DataFrame with grouped data containing first and last close columns
    :param first_column: Name of the column with the first close price
    :param last_column: Name of the column with the last close price
    :param return_column: Name of the output column for return rate
    :return: DataFrame with calculated return rate
"""

from pyspark.sql.functions import first, last, col

def calculate_return_rate(df):

    # Add Year, Month, and Week columns
    df = df.withColumn("Year", year("Date")) \
           .withColumn("Month", month("Date")) \
           .withColumn("Week", weekofyear("Date"))

    # Calculate return rate per week
    weekly_return = df.groupBy("Stock", "Year", "Week").agg(
        first("Close").alias("First_Close"),
        last("Close").alias("Last_Close")
    ).withColumn("Weekly_Return_Rate", ((col("Last_Close") - col("First_Close")) / col("First_Close")) * 100)

    # Calculate return rate per month
    monthly_return = df.groupBy("Stock", "Year", "Month").agg(
        first("Close").alias("First_Close"),
        last("Close").alias("Last_Close")
    ).withColumn("Monthly_Return_Rate", ((col("Last_Close") - col("First_Close")) / col("First_Close")) * 100)

    # Calculate return rate per year
    yearly_return = df.groupBy("Stock", "Year").agg(
        first("Close").alias("First_Close"),
        last("Close").alias("Last_Close")
    ).withColumn("Yearly_Return_Rate", ((col("Last_Close") - col("First_Close")) / col("First_Close")) * 100)

    # Display the results
    print("Weekly Return Rate:")
    weekly_return.orderBy("Stock", "Year", "Week").show(10)

    print("Monthly Return Rate:")
    monthly_return.orderBy("Stock", "Year", "Month").show(10)

    print("Yearly Return Rate:")
    yearly_return.orderBy("Stock", "Year").show(10)

    return weekly_return, monthly_return, yearly_return

weekly_return, monthly_return, yearly_return = calculate_return_rate(combined_df)

"""The weekly returns for AAPL in 2020 were generally stable, with minor fluctuations. The highest weekly return was +3.68% in week 6, while the largest drop was -8.32% in week 9, indicating some short-term volatility. In contrast, the monthly returns exhibited greater volatility, ranging from a sharp decline of -14.89% in March 2020 to a strong recovery of +21.95% in April 2020. This highlights the significant market recovery following initial losses. Overall, early 2020 was marked by steep declines and recoveries, while later months, such as October 2020, showed declining performance, reflecting a potential market slowdown towards the end of the year.

Calculate the stock with the best return rate for a specific month or year.
    
    :param df: PySpark DataFrame containing stock data with "Date", "Stock", and "Close".
    :param start_date: The start date (e.g., "2020-01-01").
    :param period: The period to analyze ("month" or "year").
    :return: The stock with the best return rate for the specified period.
"""

from pyspark.sql.functions import col, first, last, year, month

def best_return_rate(df, start_date, period="month"):

    # Extract Year and Month from the Date column
    df = df.withColumn("Year", year("Date")).withColumn("Month", month("Date"))

    # Parse the start date
    start_year, start_month, _ = map(int, start_date.split("-"))

    # Filter data based on the specified period
    if period == "month":
        filtered_df = df.filter((col("Year") == start_year) & (col("Month") == start_month))
        group_by_cols = ["Stock", "Year", "Month"]
    elif period == "year":
        filtered_df = df.filter(col("Year") == start_year)
        group_by_cols = ["Stock", "Year"]
    else:
        raise ValueError("Invalid period. Choose 'month' or 'year'.")

    # Group data and calculate return rate
    return_rate_df = filtered_df.groupBy(group_by_cols).agg(
        first("Close").alias("First_Close"),
        last("Close").alias("Last_Close")
    ).withColumn("Return_Rate", ((col("Last_Close") - col("First_Close")) / col("First_Close")) * 100)

    # Find the stock with the best return rate
    best_stock = return_rate_df.orderBy(col("Return_Rate").desc()).limit(1)

    # Show the result
    best_stock.show()

    return best_stock

# Find the stock with the best return rate for January 2020
best_stock = best_return_rate(combined_df, "2020-01-01", period="month")

# Find the stock with the best return rate for 2020
best_stock_year = best_return_rate(combined_df, "2020-01-01", period="year")

"""For January 2020, Tesla achieved the highest return rate of 51.20%, indicating significant growth in its stock price during the month. For the entire year of 2020, Tesla maintained its position as the top-performing stock with a staggering annual return rate of 720.05%, highlighting its remarkable performance and market demand throughout the year."""