from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Total value
def get_total(df: DataFrame, value_column: str) -> int:
    """
    Returns total sum of the value column
    value_column: numeric column to sum
    """
    return (
        df
        .agg(F.sum(F.col(value_column)).alias("total_count"))
        .collect()[0]["total_count"]
    )


# 2. Daily total values
def get_daily_totals(df: DataFrame, value_column: str, key_column: str, partition_column: str) -> DataFrame:
    """
    Returns a DataFrame with daily total of the value column
    partition_column: name of the derived date column
    """
    result_df = (
        df
        .groupBy(partition_column)
        .agg(F.sum(F.col(value_column)).alias("car_count"))
        .orderBy(partition_column)
    )
    return result_df

# 3. Top N periods
def get_top_n(df: DataFrame, value_column: str, n: int = 3) -> DataFrame:
    """
    Returns top N rows based on the value column
    n: number of top rows to return
    """
    window = Window.orderBy(F.col(value_column).desc())

    result_df = (   
     df.withColumn("rn", F.row_number().over(window)) 
    .select("record_time","car_count")
    .filter(F.col("rn") <= n)
    )

    return result_df 

# 4. Consecutive period with least total
def get_least_consecutive(df: DataFrame, value_column: str, key_column: str, window_size: int = 3) -> DataFrame:
    """
    Returns consecutive rows with the least sum of the value column
    window_size: number of consecutive rows to sum
    """ 

    # Define windows
    # window_spec_rn: used to assign row numbers for identifying positions
    # window_spec_rolling: used to calculate rolling sum over 3 consecutive rows
    window_spec_rn = Window.orderBy("record_time")
    window_spec_rolling = Window.orderBy("record_time").rowsBetween(-2, 0)

    # Compute rolling sum, row numbers, and window start
    df_windows = (
        df
        .withColumn("rn", F.row_number().over(window_spec_rn))                  # Assign a unique row number for ordering
        .withColumn("window_sum", F.sum("car_count").over(window_spec_rolling)) # Compute sum of car_count for current + 2 preceding rows
        .withColumn("window_start_rn", F.col("rn") - 2)                         # Identify the first row number of each 3-row window
                                                   # Exclude window size not 3
    )

    # Compute minimum window sum as a DataFrame
    min_window_sum_df = df_windows.filter(F.col("rn") >= 3) .agg(F.min("window_sum").alias("min_sum"))   

    # Join back to get all rows in minimum-sum windows
    df_min_window  = (
        df_windows.alias("full_set") 
        .join(min_window_sum_df.alias("min_set"), F.col("full_set.window_sum") == F.col("min_set.min_sum"))
        .select("full_set.window_start_rn","full_set.window_sum")
    )
    # Join back to include all 3 rows for each minimum-sum window
    result_df = (
        df_windows.alias("full_set")
        .join(
            df_min_window.alias("min_set"),
            (F.col("full_set.rn") >= F.col("min_set.window_start_rn")) &          # Include rows from start of window
            (F.col("full_set.rn") <= F.col("min_set.window_start_rn") + 2)       # Include next 2 rows to complete 3-row window
        )
        .select("full_set.record_time","full_set.car_count","min_set.window_sum")
        .orderBy("record_time")   
    )     
    return result_df
