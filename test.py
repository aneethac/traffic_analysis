def test_etl_and_analytics_summary():

    APP_NAME = "SeekTrafficAnalysis"
    input_file_path = "/Volumes/uc_0004_dev_prd_catalog/caife_utils/dropzone/test_data.txt"
    parquet_output_path = "/Volumes/uc_0004_dev_prd_catalog/caife_utils/dropzone/traffic_table"
    key_column="record_time"
    value_column="car_count"
    partition_column="record_date"
    # Define the schema for reading the space-delimited text file
    raw_input_schema = StructType([
        StructField(key_column, StringType(), False),
        StructField(value_column, StringType(), False)
    ])

    # -----------------------------
    # ETL: Read and Transform
    # -----------------------------
    df = read_and_write_to_parquet( 
        spark,
        input_path=input_file_path, 
        output_path=parquet_output_path,
        key_column=key_column, 
        value_column=value_column, 
        partition_column=partition_column,
        raw_input_schema=raw_input_schema,
        num_partitions=num_partitions
    )

    # List of checks to perform 
    selected_checks = [
        "schema_check",
        "null_filter_check",
        "total_car_count",
        "daily_totals",
        "top_n",
        "least_consecutive",
        "duplicate_check",
        "etl_record_count"
    ]

    summary = []

    # 1. Schema check 
    if "schema_check" in selected_checks:
        expected_schema = {key_column: TimestampType, value_column: IntegerType, partition_column: DateType}
        schema_pass = all(isinstance(f.dataType, expected_schema[f.name]) for f in df.schema.fields)
        summary.append(("schema_check",  "PASS" if schema_pass else "FAIL"))

    # 2. Null filter check 
    if "null_filter_check" in selected_checks:
        null_pass = df.filter(df[key_column].isNull() | df[value_column].isNull()).count() == 0
        summary.append(("null_filter_check",  "PASS" if null_pass else "FAIL"))
    # 3. Duplicate check (idempotent) 
    if "duplicate_check" in selected_checks:
        duplicate_count = df.groupBy(key_column).count().filter("count > 1").count()
        dup_pass = duplicate_count == 0
        summary.append(("duplicate_check", "PASS" if dup_pass else "FAIL"))

    # 4. ETL record count 
    if "etl_record_count" in selected_checks:
        record_count_actual = df.count()
        record_count_expected = 8 # non-null rows in test file
        summary.append(("etl_record_count", "PASS" if record_count_actual == record_count_expected else "FAIL"))
    # 5. Total car count 
    if "total_car_count" in selected_checks:
        total_actual = get_total(df, value_column)
        total_expected = 5 + 12 + 14 + 7 + 6 + 9 + 9
        summary.append(("total_car_count", "PASS" if total_actual == total_expected else "FAIL"))

    # 6. Daily totals 
    if "daily_totals" in selected_checks:
        actual_df = (
            get_daily_totals(df, "car_count", "record_time", "record_date")
            .select("record_date", "car_count")
        )
        expected_df = spark.createDataFrame(
        [
            ("2021-12-01", 40),
            ("2021-12-05", 22) 
        ],
        ["record_date", "car_count"]
        )

        pass_flag, diff_df = compare_dataframes_full_outer(
            actual_df=actual_df,
            expected_df=expected_df,
            join_cols=["record_date"],
            compare_cols=[("car_count", "car_count")]
        )
        summary.append(("daily_totals", "PASS" if pass_flag else "FAIL"))

    # 7. Top-N 
    if "top_n" in selected_checks:
        actual_df = (
            get_top_n(df, "car_count",  3)
            .select("record_time", "car_count")
        )
        expected_df = spark.createDataFrame(
           [
            ("2021-12-01T06:00:00", 14),
            ("2021-12-01T05:30:00", 12),
            ("2021-12-01T15:00:00", 9),
        ],
            ["record_time", "car_count"]
        )

        pass_flag, diff_df = compare_dataframes_full_outer(
            actual_df=actual_df,
            expected_df=expected_df,
            join_cols=["record_time"],
            compare_cols=[("car_count", "car_count")]
        )
        summary.append(("top_n", "PASS" if pass_flag else "FAIL"))

    # 9. Least consecutive 
    if "least_consecutive" in selected_checks:
        actual_df = (
            get_least_consecutive(df, "car_count", "record_time", 3)
            .select("record_time", "car_count", "window_sum")
        )
        expected_df = spark.createDataFrame(
            [
            ("2021-12-01T06:00:00", 14, 30),
            ("2021-12-01T15:00:00", 9, 30),
            ("2021-12-05T11:30:00", 7, 30),
            ],
            ["record_time", "car_count", "window_sum"]
        )

        pass_flag, diff_df = compare_dataframes_full_outer(
            actual_df=actual_df,
            expected_df=expected_df,
            join_cols=["record_time"],
            compare_cols=[("car_count", "car_count")]
        )
        summary.append(("least_consecutive", "PASS" if pass_flag else "FAIL"))

    # Show summary 
    summary_df = spark.createDataFrame(summary, ["check_category", "status"])
    summary_df.show(truncate=False)
    
