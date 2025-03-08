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
   
        from pyspark.sql.functions import col, when, lit, sum as spark_sum
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
        
        # Obtener la sesión de Spark
        spark = df.sparkSession
        
        # Mostrar el recuento de valores nulos antes de la limpieza
        print("Number of nulls before cleaning:")
        null_counts = []
        for c in df.columns:
            null_counts.append(spark_sum(col(c).isNull().cast("int")).alias(c))
        df.select(null_counts).show()
        
        # Reemplazar TODOS los valores nulos
        clean_df = df.na.fill({
            "show_id": "",
            "type": "",
            "title": "",
            "director": "Not Given",
            "country": "Unknown",
            "date_added": "2000-01-01",
            "rating": "Not Rated",
            "duration": "0 min",
            "listed_in": "Uncategorized"
        })
        
        # Asegurarse de que release_year sea entero
        clean_df = clean_df.withColumn(
            "release_year", 
            when(col("release_year").cast("int").isNotNull(), 
                col("release_year").cast("int")
            ).otherwise(0)
        )
        
        # Convertir date_added a formato de fecha
        clean_df = clean_df.withColumn(
            "date_added",
            when(col("date_added").isNull(), lit("2000-01-01").cast("date"))
            .when(col("date_added") == "", lit("2000-01-01").cast("date"))
            .otherwise(when(col("date_added").cast("date").isNotNull(), 
                        col("date_added").cast("date"))
                    .otherwise(lit("2000-01-01").cast("date")))
        )
        
        # Mostrar el recuento de valores nulos después de la limpieza
        print("Number of nulls after cleaning:")
        null_counts_after = []
        for c in clean_df.columns:
            null_counts_after.append(spark_sum(col(c).isNull().cast("int")).alias(c))
        clean_df.select(null_counts_after).show()
        
        # En lugar de intentar convertir tipos aquí, usaremos SQL para tener más control
        clean_df.createOrReplaceTempView("netflix_temp")
        
        # Definir consulta SQL para aplicar el esquema correcto
        sql_query = """
        SELECT 
            show_id,
            type,
            title,
            director,
            country,
            date_added,
            CAST(release_year AS INT) as release_year,
            rating,
            duration,
            listed_in
        FROM netflix_temp
        """
        
        # Ejecutar consulta
        result_df = spark.sql(sql_query)
        
        # Forzar el esquema con las propiedades nullable correctas
        # Registramos el resultado como vista temporal
        result_df.createOrReplaceTempView("netflix_result")
        
        # Utilizamos DDL para definir el esquema correcto
        spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW netflix_final AS
        SELECT 
            show_id,
            type,
            title,
            director AS director_non_null,
            country AS country_non_null,
            date_added,
            release_year AS release_year_non_null,
            rating AS rating_non_null,
            duration AS duration_non_null,
            listed_in AS listed_in_non_null
        FROM netflix_result
        """)
        
        # Recuperar el DataFrame final
        final_df = spark.table("netflix_final")

        # Renombrar las columnas para quitar el sufijo _non_null
        final_df = final_df \
            .withColumnRenamed("director_non_null", "director") \
            .withColumnRenamed("country_non_null", "country") \
            .withColumnRenamed("release_year_non_null", "release_year") \
            .withColumnRenamed("rating_non_null", "rating") \
            .withColumnRenamed("duration_non_null", "duration") \
            .withColumnRenamed("listed_in_non_null", "listed_in")
        
        print(f"Filas originales: {df.count()}, Filas después de limpieza: {final_df.count()}")
        
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
