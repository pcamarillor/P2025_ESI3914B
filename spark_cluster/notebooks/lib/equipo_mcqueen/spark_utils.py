from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

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