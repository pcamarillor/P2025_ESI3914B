from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType, LongType, ShortType, DoubleType, TimestampType, BinaryType
from pyspark.sqk import DataFrame

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
            "binary": BinaryType()
        }

        fields = [StructField(data, types_dic[data_type], True) for data, data_type in columns_info]
    
        return StructType(fields)
        
    @staticmethod
    def clean_df(df: DataFrame) -> DataFrame:
        return df.dropna()
    
    @staticmethod
    def write_df(df: DataFrame, path: str):
        df.write.partitionBy("release_year").parquet(path, mode="overwrite")