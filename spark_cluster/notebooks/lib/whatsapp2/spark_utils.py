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


