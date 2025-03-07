from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType, LongType, ShortType, DoubleType, TimestampType, BinaryType, ByteType, DecimalType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dic = {
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
            "byte": ByteType(),
            "decimal": DecimalType()
        }

        fields = [StructField(data, types_dic[data_type], True) for data, data_type in columns_info]
    
        return StructType(fields)
        
        