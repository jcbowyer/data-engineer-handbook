from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count, desc

# Disable automatic broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
 
# Read data
medals = spark.read.csv("/home/iceberg/data/medals.csv", header=True, inferSchema=True)
maps = spark.read.csv("/home/iceberg/data/maps.csv", header=True, inferSchema=True)
matches = spark.read.csv("/home/iceberg/data/matches.csv", header=True, inferSchema=True)
match_details = spark.read.csv("/home/iceberg/data/match_details.csv", header=True, inferSchema=True)
medals_matches_players = spark.read.csv("/home/iceberg/data/medals_matches_players.csv", header=True, inferSchema=True)


# Create bucketed tables
spark.sql("DROP TABLE IF EXISTS bootcamp.matches")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches (
    match_id STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    mapid STRING
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")

spark.sql("DROP TABLE IF EXISTS bootcamp.match_details")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")

spark.sql("DROP TABLE IF EXISTS bootcamp.medal_matches_players")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players (
    match_id STRING,
    player_id STRING,
    medal_id INTEGER
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")

spark.sql("DROP TABLE IF EXISTS bootcamp.medal_matches_players")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players (
    match_id STRING,
    player_id STRING,
    medal_id INTEGER,
    count INTEGER
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
""")


matches_df = matches.select(
    "match_id",
    "is_team_game",
    "playlist_id",
    "completion_date",
    "mapid"
)

# Insert data into bucketed tables
matches_df.write.mode("append").saveAsTable("bootcamp.matches")
 
match_details =  match_details.select(
    col("match_id").cast("string"),
    col("player_gamertag").cast("string"),
    col("player_total_kills").cast("integer"),
    col("player_total_deaths").cast("integer")
)
# Insert data into bucketed tables
match_details.write.mode("append").saveAsTable("bootcamp.match_details")
 

# Average kills per player
avg_kills = match_details.groupBy("player_gamertag").agg(
    avg("player_total_kills").alias("avg_kills"),
    count("*").alias("total_games")
).filter(col("total_games") > 10).orderBy(desc("avg_kills")).limit(10)

# Most played playlist
top_playlists = matches.groupBy("playlist_id").count().orderBy(desc("count"))

# Most played map
top_maps = matches.join(broadcast(maps), matches["mapid"] == maps["mapid"]) \
    .groupBy("name").count().orderBy(desc("count"))

# Maps with most Killing Spree medals
# Ensure 'name' is from the medals DataFrame
killing_spree_maps = matches.join(broadcast(maps), matches["mapid"] == maps["mapid"]) \
    .join(medals_matches_players, "match_id") \
    .join(broadcast(medals), "medal_id") \
    .filter(medals["name"] == "Killing Spree") \
    .groupBy(maps["name"]).count().orderBy(desc("count"))

# Optimize with sortWithinPartitions
top_playlists_sorted = top_playlists.sortWithinPartitions("playlist_id")
top_maps_sorted = top_maps.sortWithinPartitions("name")

# Show results
print("Top 10 Players by Average Kills:")
avg_kills.show(100, truncate=False)

print("\nMost Played Playlists:")
top_playlists.show(100, truncate=False)

print("\nMost Played Maps:")
top_maps.show(100, truncate=False)

print("\nMaps with Most Killing Spree Medals:")
killing_spree_maps.show(100, truncate=False)
