from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ShortType
from pyspark.sql.types import MapType
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import BooleanType


class SparkUtils:
    @staticmethod
    def generate_schema(columns):

        types_dict = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
            "array": ArrayType(StringType()),  
            "map": MapType(
                StringType(), StringType()
            ), 
            "struct": StructType(),
        }

        struct = []

        for nombre, tipo in columns:
            dato = types_dict.get(tipo)

            if dato is None:
                print(f"⚠️ Unrecognized data type: '{tipo}' for column '{nombre}'")
            struct.append(StructField(nombre, dato, True))

        return StructType(struct)
    
    @staticmethod
    def clean_df(df):
        # Mostrar el recuento de valores nulos antes de la limpieza
        print("Number of nulls before cleaning:")
        df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
        
        # Columnas que no deben tener valores nulos según la especificación
        non_nullable_columns = ["director", "country", "rating", "duration", "listed_in", "release_year"]
        
        # Crear un nuevo DataFrame con valores nulos reemplazados en columnas non-nullable
        clean_df = df.na.fill({
            "director": "Not Given",
            "country": "Unknown",
            "rating": "Not Rated",
            "duration": "0 min",
            "listed_in": "Uncategorized",
            "release_year": 0
        })
        
        # Mostrar el recuento de valores nulos después de la limpieza
        print("Number of nulls after cleaning:")
        clean_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
        
        # Definir el esquema según la especificación
        expected_schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("director", StringType(), False),
            StructField("country", StringType(), False),
            StructField("rating", StringType(), False),
            StructField("duration", StringType(), False),
            StructField("listed_in", StringType(), False),
            StructField("release_year", IntegerType(), False),
            StructField("date_added", DateType(), True)
        ])
        
        # Asegurarnos de que no hay valores nulos en las columnas que no deben tenerlos
        for column in non_nullable_columns:
            assert clean_df.filter(col(column).isNull()).count() == 0, f"Hay valores nulos en {column}"
        
        # Intentar crear el DataFrame con el esquema correcto
        rows = clean_df.collect()
        final_df = spark.createDataFrame(rows, expected_schema)
        
        return final_df
    
    @staticmethod
    def write_df(df, path, format="parquet", mode="overwrite", partition_by="release_year"):
        # Particionar por release_year como se requiere
        df.write \
          .format(format) \
          .mode(mode) \
          .partitionBy(partition_by) \
          .save(path)
        
        print(f"DataFrame escrito exitosamente en {path} en formato {format}, particionado por {partition_by}")
