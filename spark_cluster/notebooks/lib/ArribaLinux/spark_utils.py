from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType, LongType, ShortType, DoubleType, TimestampType, BinaryType, ByteType, DecimalType, ArrayType, DataType, MapType, NullType, CharType, VarcharType, DayTimeIntervalType, YearMonthIntervalType

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
            "decimal": DecimalType(),
            "array": lambda element_type: ArrayType(element_type),
            "data": DataType(),
            "map": lambda key_type, value_type: MapType(key_type, value_type),
            "null": NullType(),
            "char": CharType(),
            "varchar": VarcharType(),
            "dayTimeInterval": DayTimeIntervalType(),
            "yearMonthInterval": YearMonthIntervalType()
        }

        fields = [StructField(data, types_dic[data_type], True) for data, data_type in columns_info]
    
        return StructType(fields)
        
        