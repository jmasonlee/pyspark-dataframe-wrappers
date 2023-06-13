from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType, StructField


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.data = [{}]
        self.explicit_schema: StructType = StructType([])
        self.base_data = {}

    def with_base_data(self, **kwargs) -> "TestDataFrame":
        self.base_data = kwargs
        return self

    def set_type_for_column(self, column: str, type: DataType) -> "TestDataFrame":
        self.explicit_schema.add(column, type)
        return self

    def with_schema_from(self, reference_df: DataFrame) -> "TestDataFrame":
        self.explicit_schema = reference_df.schema
        return self

    def create_spark_df(self) -> DataFrame:
        # | only in 3.9+ - What version does chispa support?
        # What happens
        spark_data = [self.base_data | row for row in self.data]
        dataframe = self.spark.createDataFrame(data=spark_data)

        if self.explicit_schema.fields:
            for column in self.explicit_schema.fields:
                dataframe = dataframe.withColumn(column.name, col(column.name).cast(column.dataType))

        return dataframe

    def with_data(self, rows: List[Dict]) -> "TestDataFrame":
        self.data = rows
        return self

    def create_test_dataframe(self, **kwargs) -> "TestDataFrame":
        column_name = list(kwargs.keys())[0]
        column_values = kwargs[column_name]

        new_rows = []
        for row_from_column in column_values:
            new_rows.append({column_name: row_from_column})

        return self.with_data(new_rows)

    def create_test_dataframe_from_table(self, table) -> "TestDataFrame":
        table_df = self._df_from_string(table)
        self.explicit_schema = table_df.schema
        self.data = [row.asDict() for row in table_df.collect()]
        print(self.explicit_schema)
        return self

    def _df_from_string(self, table) -> DataFrame:
        rows = table.strip().split('\n')
        rdd = self.spark.sparkContext.parallelize(rows)
        df = self.spark.read.options(delimiter='|', header=True, ignoreLeadingWhiteSpace=True,
                                     ignoreTrailingWhiteSpace=True, inferSchema=True).csv(rdd)
        # for struct_field in df.schema:
        #     if struct_field.name in column_list:
        #         struct_field.nullable = nullable
        # df_mod = spark.createDataFrame(df.rdd, df.schema)

        new_schema = df.schema
        for struct_field in df.schema:  # type: StructField
            if struct_field.name.endswith("!"):
                new_schema[struct_field.name].nullable = False
        return self.spark.createDataFrame(data=df.rdd, schema=new_schema)


def create_empty_df(spark, schema=None):
    data = [{}] if schema is None else []
    return spark.createDataFrame(schema=schema, data=data).na.drop("all")
