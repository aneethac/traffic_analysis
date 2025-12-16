# Define schema for input text file
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date
from typing import List, Tuple
 
# Generic utility: Full outer join DataFrame comparison 
def compare_dataframes_full_outer(
    actual_df,
    expected_df,
    join_cols: List[str],
    compare_cols: List[Tuple[str, str]]
):
    """
    Compare two Spark DataFrames using full outer join.

    Returns:
        pass_flag (bool)
        diff_df (DataFrame containing mismatches)
    """

    joined_df = (
        actual_df.alias("a")
        .join(expected_df.alias("e"), on=join_cols, how="full_outer")
    )

    mismatch_expr = None
    for a_col, e_col in compare_cols:
        condition = (
            F.col(f"a.{a_col}").isNull()
            | F.col(f"e.{e_col}").isNull()
            | (F.col(f"a.{a_col}") != F.col(f"e.{e_col}"))
        )
        mismatch_expr = condition if mismatch_expr is None else (mismatch_expr | condition)

    diff_df = joined_df.withColumn(
        "is_mismatch", mismatch_expr
    ).filter("is_mismatch = true")

    return diff_df.count() == 0, diff_df
