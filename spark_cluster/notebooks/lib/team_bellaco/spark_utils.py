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
    # Registrar el DataFrame original como vista temporal
        df.createOrReplaceTempView("netflix_raw")
    
     # Crear una tabla temporal con el esquema deseado
        spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW netflix_clean AS
        SELECT
            show_id,
            type,
            title,
            COALESCE(director, 'Not Given') AS director NOT NULL,
            COALESCE(country, 'Unknown') AS country NOT NULL,
            COALESCE(rating, 'Not Rated') AS rating NOT NULL,
            COALESCE(duration, '0 min') AS duration NOT NULL,
            COALESCE(listed_in, 'Uncategorized') AS listed_in NOT NULL,
            COALESCE(release_year, 0) AS release_year NOT NULL,
            date_added
        FROM netflix_raw
        """)
    
    # Recuperar el DataFrame limpio con el esquema correcto
        clean_df = spark.table("netflix_clean")
    
    # Mostrar el recuento de valores nulos después de la limpieza
        print("Number of nulls after cleaning:")
        clean_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in clean_df.columns]).show()
    
        return clean_df
    
    
    @staticmethod
    def write_df(df, path, format="parquet", mode="overwrite", partition_by=None):
        writer = df.write.format(format).mode(mode)
        
        # Si se especifica partición, aplicarla
        if partition_by:
            writer = writer.partitionBy(partition_by)
            partition_info = f" particionado por {partition_by}"
        else:
            partition_info = ""
            
        # Guardar el DataFrame
        writer.save(path)
        
        print(f"DataFrame escrito exitosamente en {path} en formato {format}{partition_info}")
