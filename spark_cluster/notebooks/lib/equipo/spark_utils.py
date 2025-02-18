from pyspark.sql.types import *


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
"BinaryType": BinaryType(),
"ArrayType": ArrayType,
"MapType" : MapType,
"StructType": StructType
}

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = [StructField(name, types[type_name], True) for name, type_name in columns_info]
        return StructType(fields)