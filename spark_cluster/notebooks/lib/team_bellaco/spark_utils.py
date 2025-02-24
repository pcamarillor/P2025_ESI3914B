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
