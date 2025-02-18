from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dic = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType()
        }

        fields = [StructField(data, types_dic[data_type], True) for data, data_type in columns_info]
    
        return StructType(fields)
        
        