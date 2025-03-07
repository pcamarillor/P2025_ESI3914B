from pyspark.sql.types import StructType ,StructField, StringType, IntegerType,FloatType,DateType,BooleanType,DoubleType,ByteType,TimestampType,ShortType,BinaryType,LongType,DecimalType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        struct_type_dict = {
                            "byte":ByteType(),
                            "string": StringType(),
                            "float": FloatType(),
                            "integer": IntegerType(),
                            "bool": BooleanType(),
                            "date": DateType(),
                            "double": DoubleType(),
                            "timestamp":TimestampType(),
                            "short": ShortType(),
                            "binary":BinaryType(),
                            "long": LongType(),
                            "decimal": DecimalType()
                        }
        struct_list=[]
        for col in columns_info:
           st= StructField(col[0], struct_type_dict[col[1]], True)
           struct_list.append(st)
           
        return StructType(struct_list)
        
        
        
         