from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, ArrayType, BooleanType
from pyspark.sql.functions import col, lit, when, collect_list, struct, row_number
from pyspark.sql.window import Window
import uuid

def transform_actors_table(spark, actor_films_df):
    """
    Transforms the actor_films_df to retain only the most recent year's films per actor
    and generates the actors table conforming to the defined schema.
    
    Args:
        spark (SparkSession): The Spark session.
        actor_films_df (DataFrame): Input DataFrame with actor films data.

    Returns:
        DataFrame: Transformed DataFrame conforming to the actors schema.
    """
    # Define schema for film_struct
    film_struct_schema = StructType([
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DecimalType(3, 1), True),
        StructField("year", IntegerType(), True)
    ])

    # Define schema for actors table
    actors_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("films", ArrayType(film_struct_schema), True),
        StructField("is_active", BooleanType(), True)
    ])

    # Step 1: Assign row numbers based on year descending order for each actor
    window_spec = Window.partitionBy("actorId").orderBy(col("year").desc())
    actor_films_with_row_num = actor_films_df.withColumn("row_num", row_number().over(window_spec))

    # Step 2: Filter to keep only the most recent year (row_num = 1)
    most_recent_films_df = actor_films_with_row_num.filter(col("row_num") == 1)

    # Step 3: Aggregate films and generate the result DataFrame
    result_df = (
        most_recent_films_df.groupBy("actorId", "actor")
        .agg(
            collect_list(struct("film", "votes", "rating", "year")).alias("films"),  # Collect films for the most recent year
        )
        .withColumn("is_active", lit(True))  # Set is_active to True
        .withColumn("id", col("actorId"))  # Use actorId as the id column
        .select("id", col("actor").alias("name"), "films", "is_active")  # Select columns for the actors table
    )

    # Cast result_df to the actors schema
    actors_df = spark.createDataFrame(result_df.rdd, schema=actors_schema)

    return actors_df


# Example usage
if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("PySpark Example").getOrCreate()

    # Pre-generate UUIDs for each actor
    actor_names = [f"Actor {i}" for i in range(1, 6)]  # 5 actors
    actor_uuid_map = {name: str(uuid.uuid4()) for name in actor_names}

    # Generate sample data
    actor_films_data = [
        {
            "actorId": actor_uuid_map[f"Actor {i // 5 + 1}"],  # Use the same UUID for the same actor name
            "actor": f"Actor {i // 5 + 1}",
            "film": f"Film {i}",
            "votes": (i + 1) * 10,
            "rating": Decimal(5.0 + (i % 5) * 0.5),
            "year": 2020 + (i % 5),
        }
        for i in range(25)
    ]

    # Create DataFrame
    actor_films_df = spark.createDataFrame(actor_films_data)

    # Transform the actors table
    actors_df = transform_actors_table(spark, actor_films_df)

    # Show the resulting actors table
    actors_df.show(truncate=False)

    # Optionally, save the actors_df as a table (if using a metastore)
    # actors_df.write.mode("overwrite").saveAsTable("actors")

