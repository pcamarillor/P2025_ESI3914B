from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info):
        # Diccionario de tipos de datos soportados
        #Aqui nomas tenemos que adicionar por si no hubiera un type muy especifico pero creo estan los necesarios
        types_dict = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "FloatType": FloatType(),
            "DoubleType": DoubleType(),
            "BooleanType": BooleanType(),
        }
        
        # Creación dinámica de los StructField
        fields = [StructField(name, types_dict[data_type], True) for name, data_type in columns_info]
        
        return StructType(fields)

# Ejemplo de uso:
schema = SparkUtils.generate_schema([
    ("name", "StringType"),
    ("age", "IntegerType"),
    ("city", "StringType")
])

print(schema)
