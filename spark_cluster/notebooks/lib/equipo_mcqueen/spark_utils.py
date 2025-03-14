from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType
class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        # Follow recommendation from slides: store all types in a dictionary
        # More complex types like ArrayType, MapType are excluded for simplicity purposes
        types_dic = {
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
        
        # Store StructField objects in list
        schema = []

        # Iterate over every column and its data type
        for column, data_type in columns_info:
            # Append StructField to list
            schema.append(StructField(column, types_dic[data_type], True))

        # Return the schema
        return StructType(schema)
    
    @staticmethod
    def clean_df(df):
        # Remove rows with null values
        df = df.na.drop()
        return df
    
    @staticmethod
    def write_df(df, partition, output_path) -> None:
        # Follow class slides sample code
        df.write.mode("overwrite").partitionBy(partition).parquet(output_path)