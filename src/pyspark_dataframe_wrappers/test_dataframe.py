from dataclasses import dataclass
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType, StructField


@dataclass
class FixedColumn():
    name: str
    type: DataType
    value: Any


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.data = [{}]
        self.explicit_schema: StructType = StructType([])
        self.base_schema: StructType=StructType([])
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
        spark_data = [self.base_data | row for row in self.data]
        dataframe = self.spark.createDataFrame(data=spark_data)

        if self.explicit_schema.fields:
            for column in self.explicit_schema.fields:
                dataframe = dataframe.withColumn(column.name, col(column.name).cast(column.dataType))

        return dataframe

    def with_test_data(self, **kwargs) -> "TestDataFrame":
        rows = convert_test_data_to_rows(kwargs)
        self.data = self.combine_base_data_with_test_row_data(rows)
        return self

    def combine_base_data_with_test_row_data(self, rows):
        new_rows = []
        for row in rows:
            new_rows.append({
                **row,
                **self.base_data
            })
        return new_rows

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

    def with_fixed_column(self, fixed_column: FixedColumn) -> "TestDataFrame":
        self.explicit_schema.add(fixed_column.name, fixed_column.type)
        self.base_data[fixed_column.name] = fixed_column.value
        return self

def create_empty_df(spark, schema=None):
    data = [{}] if schema is None else []
    return spark.createDataFrame(schema=schema, data=data).na.drop("all")


def convert_test_data_to_rows(test_data: Dict) -> List[Dict]:
    if not test_data:
        return []
    length = len(list(test_data.values())[0])
    assert all(len(v) == length for v in test_data.values()), "All rows in test data must be the same length"

    results = [{} for _ in range(length)]
    for index in range(len(results)):
        for key in list(test_data.keys()):
            results[index][key] = test_data[key][index]

    return results