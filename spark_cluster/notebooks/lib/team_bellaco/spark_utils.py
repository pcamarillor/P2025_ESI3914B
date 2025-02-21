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
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "ShortType":ShortType(),
        "FloatType": FloatType(),
        "DoubleType": DoubleType(),
        "BooleanType": BooleanType(),
        "DateType": DateType(),
        "TimestampType": TimestampType(),
        "BinaryType": BinaryType(),
        "ArrayType": ArrayType(StringType()),
        "MapType": MapType(StringType(), StringType()),
        "StructType": StructType()
        }
        struct = []
        
        for nombre, tipo in columns:
            dato = types_dict.get(tipo)
            struct.append(StructField(nombre, dato, True))
        
        return StructType(struct)

