Medical Insurance Data Cleaning with PySpark
This project demonstrates a simple data cleaning and transformation pipeline using Apache Spark with PySpark. The goal is to prepare a medical insurance dataset for analysis or machine learning by standardizing and enriching the data.

🔧 What the Code Does
Loads raw data from medical_insurance.csv.
Standardizes text columns like gender, region, and discount_eligibility by trimming and converting to lowercase.
Categorizes BMI into:
Underweight
Normal
Overweight
Obese
Flags seniors with a new column is_senior (1 if age ≥ 60, else 0).
Creates a new ratio column expense_to_premium_ratio (expenses divided by premium).
Writes the cleaned and transformed data back to a new CSV file cleaned_medical_insurance.csv.
💻 Technologies Used
Python
PySpark
VS Code
📁 Output
The cleaned dataset is saved to:
