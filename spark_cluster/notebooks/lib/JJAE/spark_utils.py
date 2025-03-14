from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ShortType, 
    DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType
)

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        # Store all types in a dictionary
        types_dic = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "LongType": LongType(),
            "ShortType": ShortType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "BinaryType": BinaryType()
        }

        # Create schema
        schema = [StructField(column, types_dic[data_type], True) for column, data_type in columns_info]
        return StructType(schema)