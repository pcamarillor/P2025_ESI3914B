from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType, LongType, ShortType, DoubleType, TimestampType, BinaryType, ByteType, DecimalType, ArrayType, DataType, MapType, NullType, CharType, VarcharType, DayTimeIntervalType, YearMonthIntervalType
from pyspark.sql import DataFrame


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
            "char": lambda length=1: CharType(length),
            "varchar": lambda length=255: VarcharType(length),
            "dayTimeInterval": DayTimeIntervalType(),
            "yearMonthInterval": YearMonthIntervalType()
        }

        fields = [StructField(data, types_dic[data_type], True) for data, data_type in columns_info]
    
        return StructType(fields)
        
    @staticmethod
    def clean_df(df: DataFrame) -> DataFrame:
        # List of columns to check for the 'Not Given' value
        columns_to_check = ['show_id', 'director', 'type', 'country', 'date', 'rating', 'duration', 'listed_in']
        
        # Filter out rows where any of the specified columns have the value 'Not Given'
        for col_name in columns_to_check:
            df = df.filter(df[col_name] != 'Not Given')
        
        return df
    
    @staticmethod
    def write_df(df: DataFrame, path: str):
        df.write.partitionBy("release_year").parquet(path, mode="overwrite")