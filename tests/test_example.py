import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType


def apple(spark, df):
    # Make an asseration that involves the apples column
    assert df.filter(col("apples").isNotNull()).count() != 0
    # Create a new column called fizzbuzz that fizz if the number in the number column is divisible by 3
    # the fizzbuzz column should be "buzz" if the number in the number column is divisible by 5
    # the fizzbuzz columns should be "fizzbuzz" if the number in the number column is dividialbe by 15

def test_df_always_has_apples(spark):
    input_df = spark.createDataFrame(
        schema=StructType([StructField("apples", StringType())]),
        data=[{"apples": None}])

    with pytest.raises(AssertionError):
        apple(spark, input_df)

