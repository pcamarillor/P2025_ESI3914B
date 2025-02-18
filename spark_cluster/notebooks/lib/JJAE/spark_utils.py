import findspark
from pyspark.sql.types import StructType
findspark.init()

class SparkUtils:

    self.types = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "ShortType": ShortType(),
        "DoubleType": DoubleType(),
        "FloatType": FloatType(),
        "BooleanType": BooleanType(),
        "DateType": DateType(),
        "TimestampType": TimestampType(),
        "BinaryType": BinaryType()
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        struct = []
        for column in columns_info:
            struct.append(StructField(column[0], self.types[column[1]], True))

        return StructType(struct)


# Test
sample_data = [
    ("id", "IntegerType" ),
    ("name", "StringType" ),
    ("age", "IntegerType" ),
    ("mail", "StringType" ),
    ("phone", "StringType" ),
    ("created_at", "TimestampType"),
    ("height", "FloatType"),
]

schema = SparkUtils.generate_schema(sample_data)
print(schema)