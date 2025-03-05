from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SampleSparkApp").getOrCreate()

# Define hardcoded data
data = [
    ("Alice", 30, "New York"),
    ("Bob", 24, "Los Angeles"),
    ("Charlie", 28, "Chicago")
]

# Define schema (column names)
columns = ["name", "age", "city"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the original data
print("Original Data:")
df.show()

# Filter: Select rows where age > 25
filtered_df = df.filter(col("age") > 25)

# Show filtered data
print("Filtered Data (age > 25):")
filtered_df.show()

# Stop Spark session
spark.stop()



#docker run -it --rm spark:python3 /opt/spark/bin/pyspark
