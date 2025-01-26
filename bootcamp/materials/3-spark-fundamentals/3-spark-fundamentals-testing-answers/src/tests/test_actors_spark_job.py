import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import *
from decimal import Decimal
from uuid import uuid4
from jobs.actors_spark_job import transform_actors_table


@pytest.fixture(scope="module")
def spark():
    """Fixture for creating a SparkSession."""
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


# Define the test function for data transformation
def test_actors_transformation(spark):
    # Define schema for input data
    input_schema = StructType([
        StructField("actorId", StringType(), True),
        StructField("actor", StringType(), True),
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DecimalType(3, 1), True),
        StructField("year", IntegerType(), True)
    ])

    # Define source data
    source_data = [
        ("1", "Actor 1", "Film A1", 100, Decimal(9.0), 2022),
        ("1", "Actor 1", "Film A2", 110, Decimal(8.5), 2021),
        ("2", "Actor 2", "Film B1", 120, Decimal(7.0), 2022),
        ("3", "Actor 3", "Film C1", 90, Decimal(6.5), 2023),
        ("3", "Actor 3", "Film C2", 80, Decimal(6.0), 2022),
    ]
    source_df = spark.createDataFrame(source_data, schema=input_schema)

    # Apply the transformation
    actual_df = transform_actors_table(spark, source_df)

    # Define schema for expected data
    expected_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", DecimalType(3, 1), True),
            StructField("year", IntegerType(), True)
        ])), True),
        StructField("is_active", BooleanType(), True)
    ])

    # Define expected data
    expected_data = [
        ("1", "Actor 1", [{"film": "Film A1", "votes": 100, "rating": Decimal(9.0), "year": 2022}], True),
        ("2", "Actor 2", [{"film": "Film B1", "votes": 120, "rating": Decimal(7.0), "year": 2022}], True),
        ("3", "Actor 3", [{"film": "Film C1", "votes": 90, "rating": Decimal(6.5), "year": 2023}], True),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


# Define a second test function for schema validation
def test_actors_schema(spark):
    # Define schema for input data
    input_schema = StructType([
        StructField("actorId", StringType(), True),
        StructField("actor", StringType(), True),
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DecimalType(3, 1), True),
        StructField("year", IntegerType(), True)
    ])

    # Define source data
    source_data = [
        ("1", "Actor 1", "Film A1", 100, Decimal(9.0), 2022),
        ("1", "Actor 1", "Film A2", 110, Decimal(8.5), 2021),
        ("2", "Actor 2", "Film B1", 120, Decimal(7.0), 2022),
        ("3", "Actor 3", "Film C1", 90, Decimal(6.5), 2023),
        ("3", "Actor 3", "Film C2", 80, Decimal(6.0), 2022),
    ]
    source_df = spark.createDataFrame(source_data, schema=input_schema)

    # Apply the transformation
    actual_df = transform_actors_table(spark, source_df)

    # Define the expected schema
    expected_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", DecimalType(3, 1), True),
            StructField("year", IntegerType(), True)
        ])), True),
        StructField("is_active", BooleanType(), True)
    ])

    # Assert schema equality
    assert actual_df.schema == expected_schema, "Schema does not match expected schema"
