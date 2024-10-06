# ---------- Section 3 ---------- #


# Question 1 - Cluster Configuration for Notebook Development
# 1) Go to Compute tab on the left and select Create Cluster.
# 2) Select whether you need Multi node or Single node. For our current use case, Single node is enough.
# 3) Sekect the runtime.
# 4) Disable autoscaling since we're working with test environment with limited data. If large datasets, then we can turn it on.
# 5) Select driver and worker (min/max) required for the project.
# 6) Create cluster.


# Question 2 - Python Code for Data Processing

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max


def extract_oldest_customers_to_delta(mongo_scope, src_conn_key, db_key, delta_path):
    """
    Extracts the oldest customer by city from a MongoDB collection and writes the result to a Delta table.

    :param mongo_scope: Databricks secret scope containing MongoDB credentials.
    :param src_conn_key: Key for the MongoDB connection string.
    :param db_key: Key for the MongoDB database name.
    :param delta_path: Delta table save path.
    """
    # Set up MongoDB connection using Databricks secrets
    mongo_uri = dbutils.secrets.get(scope=mongo_scope, key=src_conn_key)
    database_name = dbutils.secrets.get(scope=mongo_scope, key=db_key)

    # Set the MongoDB collection (customers) we want to read
    mongo_collection = f"{database_name}.customers"

    # Read data from the MongoDB collection
    df_mongo = (
        spark.read.format("mongo")
        .option("uri", mongo_uri)
        .option("database", database_name)
        .option("collection", "customers")
        .load()
    )

    # Identify the oldest customer in each city
    df_max_age = df_mongo.groupBy("City").agg(spark_max("Age").alias("Max_Age"))

    # Join the original DataFrame with the grouped DataFrame to get the oldest customer details
    df_oldest_customers = df_max_age.join(
        df_mongo,
        (df_max_age.City == df_mongo.City) & (df_max_age.Max_Age == df_mongo.Age),
    ).select("ID", "Customer_Name", "City", "Age")

    # Write the results to a Delta table
    df_oldest_customers.write.format("delta").mode("overwrite").save(delta_path)


# Main code to call the function
extract_oldest_customers_to_delta(
    mongo_scope="mongodb",
    src_conn_key="src_conn",
    db_key="database",
    delta_path="/mnt/delta/sample_data",
)


# Question 3 - Data Extraction by Date Range

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit
from cryptography.fernet import Fernet
import base64


# Function to choose the environment (dev or prod)
def get_db_secrets(env):
    """
    Get MongoDB URI and database name based on the environment (dev or prod).

    :param env: Environment name ('dev' or 'prod').
    :return: MongoDB URI and database name.
    """
    if env == "dev":
        mongo_uri = dbutils.secrets.get(scope="mongodb.dev", key="src_conn")
        database_name = dbutils.secrets.get(scope="mongodb.dev", key="database")
    elif env == "prod":
        mongo_uri = dbutils.secrets.get(scope="mongodb.prod", key="src_conn")
        database_name = dbutils.secrets.get(scope="mongodb.prod", key="database")
    else:
        raise ValueError("Invalid environment. Edit 'dev' or 'prod' only.")
    return mongo_uri, database_name


# Encryption function for Customer_Name field
def encrypt_column(df, column_name, secret_key):
    """
    Encrypts a specified column in a DataFrame using the provided secret key.

    :param df: Input DataFrame.
    :param column_name: Name of the column to encrypt.
    :param secret_key: Encryption key for the column.
    :return: DataFrame with the encrypted column.
    """
    fernet = Fernet(secret_key)
    encrypted_col = df.rdd.map(
        lambda row: (
            row.ID,
            fernet.encrypt(row[column_name].encode()).decode(),
            row.City,
            row.Age,
        )
    )
    df_encrypted = encrypted_col.toDF(["ID", "Customer_Name", "City", "Age"])
    return df_encrypted


# Combined function with extended features
def extract_oldest_customers_to_delta_with_encryption(
    env, start_date, end_date, secret_scope, secret_key, delta_path
):
    """
    Extracts customers by date range from MongoDB, finds the oldest customer per city, encrypts the Customer_Name column,
    and writes the result to Delta table.

    :param env: Environment name ('dev' or 'prod').
    :param start_date: Start date for filtering (inclusive).
    :param end_date: End date for filtering (inclusive).
    :param secret_scope: Databricks secret scope for encryption key.
    :param secret_key: Secret key name for encryption.
    :param delta_path: Delta table save path.
    """
    # Fetch MongoDB secrets
    mongo_uri, database_name = get_db_secrets(env)

    # Set the MongoDB collection (customers) we want to read
    mongo_collection = f"{database_name}.customers"

    # Read data from the MongoDB collection
    df_mongo = (
        spark.read.format("mongo")
        .option("uri", mongo_uri)
        .option("database", database_name)
        .option("collection", "customers")
        .load()
    )

    # Filter the data based on the date range
    df_filtered = df_mongo.filter(
        (col("Registration_Date") >= lit(start_date))
        & (col("Registration_Date") <= lit(end_date))
    )

    # Identify the oldest customer in each city within the date range
    df_max_age = df_filtered.groupBy("City").agg(spark_max("Age").alias("Max_Age"))

    # Join the original DataFrame with the grouped DataFrame to get the oldest customer details
    df_oldest_customers = df_max_age.join(
        df_filtered,
        (df_max_age.City == df_filtered.City) & (df_max_age.Max_Age == df_filtered.Age),
    ).select("ID", "Customer_Name", "City", "Age")

    # Fetch encryption key from secrets
    secret_key_value = dbutils.secrets.get(secret_scope, secret_key)

    # Encrypt the Customer_Name column
    df_encrypted = encrypt_column(
        df_oldest_customers, "Customer_Name", secret_key_value
    )

    # Write the encrypted data to a Delta table
    df_encrypted.write.format("delta").mode("overwrite").save(delta_path)


# Main code to call the function
extract_oldest_customers_to_delta_with_encryption(
    env="dev",  # Development or Production environment
    start_date="2022-01-01",  # Start date for filtering customers
    end_date="2023-01-01",  # End date for filtering customers
    secret_scope="encryption_scope",  # Secret scope for encryption key
    secret_key="encryption_key",  # Key name in the secret scope
    delta_path="/mnt/delta/sample_data",  # Path to save the result as a Delta table
)

# Question 4 - Scheduling and Notifications
# 1) Click on Schedule button at the top.
# 2) Fill in the name of the job,
# 3) Select Scheduled option and Every day. Select the date/time you need.
# 4) Select the cluster that we created earlier.
# 5) It is optional to place parameters in your Scheduled jobs. In this case, we could put dev or prod as your parameters.
# 6) Place your emails and check the Success button. This will send an email to notify you that the script has successfully executed.

# Question 5 - Databricks Cluster Creation from WSL Terminal
# 1) Input the following command: `databricks configure --token`
# 2) Enter the Databricks host and token
# 3) Once verified, verify using the command: `databricks clusters list`
# 4) Create a JSON file (here we will call it cluster-config.json) to define configuration of the cluster. Refer below example:
# {
#   "cluster_name": "my-cluster",
#   "spark_version": "10.4.x-scala2.12",  # Replace with desired Spark version
#   "node_type_id": "i3.xlarge",           # Replace with desired instance type
#   "autotermination_minutes": 60,
#   "num_workers": 2
# }
# 5) Use the following command to create the cluster: `clusters create --json-file cluster-config.json`
# 6) To start the cluster, enter: `databricks clusters create --json-file cluster-config.json`


# Question 6 - Implement a Spark SQL Query

# Read the MongoDB collection "customers" into a DataFrame
mongo_uri = dbutils.secrets.get(scope="mongodb", key="src_conn")
database_name = dbutils.secrets.get(scope="mongodb", key="database")

df_mongo = (
    spark.read.format("mongo")
    .option("uri", mongo_uri)
    .option("database", database_name)
    .option("collection", "customers")
    .load()
)

# Register the DataFrame as a temporary view to run SQL queries
df_mongo.createOrReplaceTempView("customers")

# Spark SQL query to calculate average, min, and max age per city
age_stats_query = """
    SELECT 
        City,
        AVG(Age) AS avg_age,
        MIN(Age) AS min_age,
        MAX(Age) AS max_age
    FROM customers
    GROUP BY City
"""

# Execute the query and store the results in a DataFrame
df_age_stats = spark.sql(age_stats_query)

# Write the results to a Delta table
df_age_stats.write.format("delta").mode("overwrite").save(
    "/mnt/delta/customer_age_stats"
)

# Question 7 - Data Transformation and Cleaning

# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import IntegerType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerDataCleansing").getOrCreate()

# Define input and output paths
input_path = "/mnt/data/raw/customers.csv"
output_path = "/mnt/delta/cleaned_customers"

# Read the CSV file into a PySpark DataFrame
df_customers = (
    spark.read.format("csv")
    .option("header", "true")  # CSV has a header row
    .option("inferSchema", "true")  # Infer schema from data
    .load(input_path)
)

# Handle missing values (drop rows with missing Customer_ID)
df_cleaned = df_customers.dropna(subset=["ID"])

# Fill missing City with "Unknown". We can fill Age to anything if it's missing too (depends on requirements)
df_cleaned = df_cleaned.fillna({"City": "Unknown"})

# Standardize formats (lowercase and trim Customer_Name, cast Age to int, cast Registration_Date to date)
df_cleaned = df_cleaned.withColumn("Customer_Name", trim(col("Customer_Name")))
df_cleaned = df_cleaned.withColumn("Age", col("Age").cast(IntegerType()))

# Write the cleaned data to a Delta table
df_cleaned.write.format("delta").mode("overwrite").save(output_path)

# Read the cleaned data from the Delta table to verify
df_cleaned_readback = spark.read.format("delta").load(output_path)
df_cleaned_readback.show(5)

# Question 8 - Automated Testing and CI/CD Pipeline
# 0) In this example, we will be using Azure DevOps (ADO) as the CI/CD pipeline.
# 1) First, set up version control in ADO. Use Azure Repos to store  Databricks notebooks as .py files.
# 2) For testing, we can use pytest which tests the file for transformation logic. Refer example below

import pytest
from pyspark.sql import SparkSession
from my_transformations import transform_data


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()


def test_transform_data(spark):
    input_data = [(1, "Alice"), (2, "Bob")]
    input_df = spark.createDataFrame(input_data, ["ID", "Name"])
    output_df = transform_data(input_df)
    expected_data = [(1, "ALICE"), (2, "BOB")]
    expected_df = spark.createDataFrame(expected_data, ["ID", "Name"])
    assert output_df.collect() == expected_df.collect()


# 3) Install Databricks CLI on ADO to automate cluster creation, job execution etc.
# 4) Use parameters for environment specific configuration. Refer below example:

env = dbutils.widgets.get("env")  # get environment parameter

if env == "prod":
    input_path = "/mnt/prod/data"
else:
    input_path = "/mnt/dev/data"


# 5) Create an ADO pipeline to automate testing and deploying Databricks jobs. We need a yaml for this.
# 6) Deploy changes to dev, staging and prod environments
