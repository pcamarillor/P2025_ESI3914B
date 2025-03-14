from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, 
    NullType, ShortType, LongType, ByteType, BinaryType, DecimalType, DateType, TimestampType, 
    TimestampNTZType, DayTimeIntervalType, ArrayType, MapType
)

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info):
        types_dict = {
            "string": StringType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "null": NullType(),
            "short": ShortType(),
            "long": LongType(),
            "byte": ByteType(),
            "binary": BinaryType(),
            "decimal": DecimalType(10, 2),
            "date": DateType(),
            "timestamp": TimestampType(),
            "timestamp_ntz": TimestampNTZType(),
            "day_time_interval": DayTimeIntervalType(),
            "array_string": ArrayType(StringType()),
            "map_string_integer": MapType(StringType(), IntegerType()),
        }
        
        fields = [
            StructField(name, types_dict[data_type], True) 
            for name, data_type in columns_info if data_type in types_dict
        ]
        
        return StructType(fields)

    @staticmethod
    def clean_df(spark: SparkSession, path: str, output_path: str):
        """Lee un CSV, elimina valores nulos y lo guarda en un nuevo CSV."""
        df = spark.read.option("header", True).csv(path)
        df_cleaned = df.dropna(how="any")
        df_cleaned.write.mode("overwrite").option("header", True).csv(output_path)
        print(f"Data cleaned and saved to {output_path}")

    @staticmethod
    def write_df(spark: SparkSession, input_path: str, output_path: str, partition_col: str):
        """Lee un CSV y lo guarda como Parquet particionado por una columna."""
        df = spark.read.option("header", True).csv(input_path)
        df.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)
        print(f"Data saved to {output_path} partitioned by '{partition_col}'")
