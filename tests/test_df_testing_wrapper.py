import pytest
from chispa import assert_df_equality
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType

from pyspark_dataframe_wrappers import FixedColumn, convert_test_data_to_rows
from pyspark_dataframe_wrappers.test_dataframe import TestDataFrame, create_empty_df


# Bug: createDataFrame returns a new dataframe without the base_data or schema of the parent dataframe
# We have no tests for exception handling
# createDataFrame shouldn't return TestDataFrame, it should return a dataframe


# want to create a dataframe specifying only the "category 1" data with "category 2" data set as default values
# we need to handle columns in category 2 that are required to be unique

# category 1: data we care about/that is being tested
# category 2: data we need but don't care about
# category 3: data we do not want/need


def test_combine_base_data_with_test_row_data(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")

    combined_data = base_data.combine_base_data_with_test_row_data(
        rows=[
            {"date": "2000-01-02 03:04:05"},
            {"date": "2000-01-01 04:05:06"}
        ]
    )

    assert combined_data == [
        {'user_id': "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05"},
        {'user_id': "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06"}
    ]


def test_convert_test_data_to_rows():
    test_data = {"name": ["apple", "banana", "pear"], "id": [1, 2, 3]}
    expected = [{"name": "apple", "id": 1}, {"name": "banana", "id": 2}, {"name": "pear", "id": 3}]
    assert convert_test_data_to_rows(test_data) == expected

def test_convert_data_row_mismatch():
    test_data = {"name": ["apple", "banana", "pear"], "id": [1, 2]}

    with pytest.raises(AssertionError) as e:
        convert_test_data_to_rows(test_data)
    assert "All rows in test data must be the same length" in str(e.value)

def test_convert_test_data_to_rows_with_empty_dict():
    test_data = {}
    assert convert_test_data_to_rows(test_data) == []

def test_with_test_data_takes_multiple_columns(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")

    test_df = base_data \
        .with_test_data(
        date=[
            "2000-01-02 03:04:05",
            "2000-01-01 04:05:06"
        ],
        comment=["first", "second"]
    )

    assert test_df.data == [
        {'user_id': "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05", "comment": "first"},
        {'user_id': "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06", "comment": "second"}
    ]

def test_with_base_data_takes_fixed_column(spark):
    fixed_column = FixedColumn(name="id", type=IntegerType(), value=1)
    test_data = (
        TestDataFrame(spark)
        .with_fixed_column(fixed_column)
        .with_test_data()
    )
    assert test_data.explicit_schema.fields == [StructField("id", IntegerType())]
    assert test_data.data == []


def test_base_data_is_applied_to_test_data_row(spark):
    fixed_column1 = FixedColumn(name="id", type=IntegerType(), value=1)
    fixed_column2 = FixedColumn(name="apple_type", type=StringType(), value='Macintosh')
    test_data = (
        TestDataFrame(spark)
        .with_fixed_column(fixed_column1)
        .with_fixed_column(fixed_column2)
        .with_test_data(comment=['first'])
    )
    assert test_data.explicit_schema.fields == [StructField("id", IntegerType()),
                                                StructField("apple_type", StringType())]
    assert test_data.data == [{'id': 1, 'comment': 'first', 'apple_type': 'Macintosh'}]


def test_create_dataframe_accepts_no_arguments(spark):
    test_data = TestDataFrame(spark).with_test_data()
    assert test_data.data == []
    assert test_data.explicit_schema.fields == []


def test_add_column_to_schema(spark):
    test_df = TestDataFrame(spark).set_type_for_column("name", StringType())
    # make sure this includes a name column of type String
    assert test_df.explicit_schema.fields[0].name == "name"
    assert test_df.explicit_schema.fields[0].dataType == StringType()


def test_multiple_columns(spark):
    ###ARRANGE###
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    ############

    ###ACT#####
    test_df = (base_data
    .create_test_dataframe_from_table(
        """
         date                | stars 
         2000-01-02 03:04:05 | 5.0     
         2000-01-01 04:05:06 | 3.0     
         2000-01-01 05:06:07 | 4.0    
        """
    ))
    #########

    expected_df = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05", "stars": 5.0},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06", "stars": 3.0},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 05:06:07", "stars": 4.0}
    ])
    expected_df = expected_df.withColumn("date", to_timestamp(expected_df.date))

    assert_df_equality(test_df.create_spark_df(), expected_df, ignore_nullable=True, ignore_column_order=True,
                       ignore_row_order=True)
    #########


def test_multiple_columns_with_same_name(spark):
    ###ARRANGE###
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    ############

    ###ACT#####
    test_df = (base_data
    .create_test_dataframe_from_table(
        """
         user_id
         test_user_1 
         test_user_2 
         test_user_3 
        """
    ))
    #########

    expected_df = spark.createDataFrame([
        {"user_id": "test_user_1", "business_id": "Crusty Crab"},
        {"user_id": "test_user_2", "business_id": "Crusty Crab"},
        {"user_id": "test_user_3", "business_id": "Crusty Crab"}
    ])

    assert_df_equality(test_df.create_spark_df(), expected_df, ignore_nullable=True, ignore_column_order=True,
                       ignore_row_order=True)
    #########


def test_multiple_columns_with_same_name_but_different_types(spark):
    ###ARRANGE###
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    ############

    ###ACT#####
    test_df = (base_data
    .create_test_dataframe_from_table(
        """
         user_id
         1.0 
         2.0 
         3.0 
        """
    ))
    #########

    expected_df = spark.createDataFrame([
        {"user_id": 1.0, "business_id": "Crusty Crab"},
        {"user_id": 2.0, "business_id": "Crusty Crab"},
        {"user_id": 3.0, "business_id": "Crusty Crab"}
    ])
    # expected_df = expected_df.withColumn("user_id", ...int)
    # expected_df = expected_df.with_expicit_schema("user_id", ...int)

    # We have a user_id that is formatted as 1_123 on mobile or 123 on www.
    # The function we want to test splits based on the underscore and returns the first half

    '''
    id          ->   id
    1_123            1
    '''
    # if col doesn't exist -> easy
    # if col exist in base data ->
    #   -> use the original base data type
    #   -> unless explicitly defined otherwise

    assert_df_equality(test_df.create_spark_df(), expected_df, ignore_nullable=True, ignore_column_order=True,
                       ignore_row_order=True)
    #########


def test_explicitly_set_column_type_for_base_data(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="12345", business_id="2468")
    base_data = base_data.set_type_for_column("user_id", LongType())
    base_data = base_data.set_type_for_column("business_id", LongType())

    actual_df = base_data.create_spark_df()

    expected_df = spark.createDataFrame([
        {"user_id": 12345, "business_id": 2468},
    ])

    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_column_order=True,
                       ignore_row_order=True)


def test_dataframe_from_string(spark):
    # I want a dataframe from a new method that we haven't made up yet that takes in a string

    new_df = TestDataFrame(spark)._df_from_string("""
            date                | stars
            2000-01-02 03:04:05 | 5
            2000-01-01 04:05:06 | 3
            2000-01-01 05:06:07 | 4
        """)

    expected_df = spark.createDataFrame(
        schema=StructType(
            [
                StructField("date", StringType()),
                StructField("stars", IntegerType()),
            ]
        ),
        data=[
            {"date": "2000-01-02 03:04:05", "stars": 5},
            {"date": "2000-01-01 04:05:06", "stars": 3},
            {"date": "2000-01-01 05:06:07", "stars": 4}
        ]
    )
    expected_df = expected_df.withColumn("date", to_timestamp(expected_df.date))
    assert_df_equality(new_df, expected_df)


def test_can_create_an_empty_df_with_a_non_nullable_field(spark):
    assert create_empty_df(spark, StructType([StructField('_', StringType(), False)])).count() == 0


def test_can_create_an_empty_df_without_a_schema(spark):
    assert create_empty_df(spark).count() == 0
