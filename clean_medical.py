from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when

# Step 1: Start Spark session
spark = SparkSession.builder.appName(
    "Medical Insurance Transformation").getOrCreate()

# Step 2: Load CSV
df = spark.read.csv("medical_insurance.csv", header=True, inferSchema=True)

# Step 3.1: Standardize categorical values to lowercase
df = df.withColumn("gender", lower(trim(col("gender")))) \
       .withColumn("region", lower(trim(col("region")))) \
       .withColumn("discount_eligibility", lower(trim(col("discount_eligibility"))))

# Step 3.2: Create new feature - BMI category
df = df.withColumn(
    "bmi_category",
    when(col("bmi") < 18.5, "underweight")
    .when((col("bmi") >= 18.5) & (col("bmi") < 25), "normal")
    .when((col("bmi") >= 25) & (col("bmi") < 30), "overweight")
    .otherwise("obese")
)

# Step 3.3: Create new column - is_senior
df = df.withColumn("is_senior", when(col("age") >= 60, "old")
                   .when((col("age") >= 30) & (col("age") < 60), "middle-aged")
                   .otherwise("young"))

# Step 3.4: Create new column - expense_to_premium_ratio
df = df.withColumn("expense_to_premium_ratio",
                   col("expenses") / col("premium"))

# Step 4: Save transformed data to CSV
df.coalesce(1).write.csv("cleaned_medical_insurance",
                         header=True, mode="overwrite")

# Step 5: Load and show cleaned data
dt = spark.read.csv("cleaned_medical_insurance",
                    header=True, inferSchema=True)
dt.show()

# Step 6: Stop Spark session
spark.stop()
# Step 7: Print schema of cleaned data
dt.printSchema()
