import findspark
from pyspark.sql.types import StructType, StructField , StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

class SparkUtils:

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types = {
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

        struct = []
        for column in columns_info:
            struct.append(StructField(column[0], types[column[1]], True))

        return StructType(struct)
