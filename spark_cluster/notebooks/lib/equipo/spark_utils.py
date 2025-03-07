from pyspark.sql.types import *


types = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "short": ShortType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "binary": BinaryType(),
}


class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = [StructField(name, types[type_name], True) for name, type_name in columns_info]
        return StructType(fields)