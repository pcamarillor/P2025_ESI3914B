from pyspark.sql.types import StructType ,StructField, StringType, IntegerType,FloatType,DateType,BooleanType,DoubleType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        struct_type_dict = {
                            "StringType": StringType(),
                            "FloatType": FloatType(),
                            "IntegerType": IntegerType(),
                            "BooleanType": BooleanType(),
                            "DateType": DateType(),
                            "DoubleType": DoubleType()
                        }
        struct_list=[]
        for col in columns_info:
           st= StructField(col[0], struct_type_dict[col[1]], True)
           struct_list.append(st)
           
        return StructType(struct_list)
        
        
        
        