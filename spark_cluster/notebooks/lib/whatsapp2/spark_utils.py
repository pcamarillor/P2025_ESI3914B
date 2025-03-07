from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, 
    NullType, ShortType, LongType, ByteType, BinaryType, DecimalType, DateType, TimestampType, 
    TimestampNTZType, DayTimeIntervalType, ArrayType, MapType
)

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info):
        # Diccionario de tipos de datos soportados
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
            "decimal": DecimalType(10, 2),  # Precisión y escala por defecto
            "date": DateType(),
            "timestamp": TimestampType(),
            "timestamp_ntz": TimestampNTZType(),
            "day_time_interval": DayTimeIntervalType(),
            "array_string": ArrayType(StringType()),  # Ejemplo de ArrayType
            "map_string_integer": MapType(StringType(), IntegerType()),  # Ejemplo de MapType
        }
        
        # Creación dinámica de los StructField
        fields = [
            StructField(name, types_dict[data_type], True) 
            for name, data_type in columns_info if data_type in types_dict
        ]
        
        return StructType(fields)
    

    def clean_df(spark,path,output_path):
        df = spark.read.option("header", True).csv(path)  # Replace with actual file path

    # Drop rows with null values
    #fillna 
        df_cleaned = df.dropna(how="any")

        df_cleaned.write.mode("overwrite").option("header", True).csv(output_path)


    def write_df(spark,input_path, output_path, partition_col):
        # Load the CSV file into a Spark DataFrame
        df = spark.read.option("header", True).csv(input_path)

        # Persist the cleaned DataFrame with a dynamic partition column
        df.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)

        print(f"Data saved to {output_path} partitioned by '{partition_col}'")
