from pyspark.sql.types import *

class SparkUtils:
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
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = [StructField(name, SparkUtils.types[type_name], True) for name, type_name in columns_info]
        return StructType(fields)
    
    @staticmethod
    def clean_df(df):
        return df.dropna()
    
    @staticmethod
    def write_df(df, partition, directory):
        df.write \
        .mode("overwrite") \
        .partitionBy(partition) \
        .parquet(directory)
