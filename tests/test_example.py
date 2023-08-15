import pytest
from chispa import assert_df_equality
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from pyspark_dataframe_wrappers import TestDataFrame


def apple(spark, df):
    # Make an asseration that involves the apples column
    assert df.filter(col("apples").isNotNull()).count() != 0
    # Create a new column called fizzbuzz that fizz if the number in the number column is divisible by 3
    df = df.withColumn("fizzbuzz", lit('fizz'))
    # the fizzbuzz column should be "buzz" if the number in the number column is divisible by 5
    # the fizzbuzz columns should be "fizzbuzz" if the number in the number column is dividialbe by 15
    return df

def test_df_always_has_apples(spark):
    input_df = spark.createDataFrame(
        schema=StructType([StructField("apples", StringType())]),
        data=[{"apples": "sauce"}])

    output_df = apple(spark, input_df)
    assert_df_equality(input_df, output_df)

def test_df_always_has_apples_with_pyspark_dataframe_wrappers(spark):
    input_df = TestDataFrame(spark).with_base_data(apples="sauce").create_spark_df()
    output_df = apple(spark, input_df)
    assert_df_equality(input_df, output_df)

def test_columns_with_3_are_fizz(spark):
    input_df = spark.createDataFrame(
        schema=StructType([
            StructField("apples", StringType()),
            StructField("number", IntegerType()),
        ]),
        data=[{"apples":"sauce", "number": 3}])

    output_df = apple(spark, input_df)

    expected_df = spark.createDataFrame(
        schema=StructType([
            StructField("apples", StringType()),
            StructField("number", IntegerType()),
            StructField("fizzbuzz", StringType())
        ]),
        data=[{"apples":"sauce", "number": 3, "fizzbuzz": "fizz"}])

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
