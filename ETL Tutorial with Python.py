# 🌟 ETL TUTORIAL WITH PANDAS 🌟  
# This script demonstrates Extract, Transform, Load (ETL) operations  
# using the schema previously shared.  
# We'll use pandas for data manipulation and numpy for numerical operations.  
# All explanations are in comments!  

# 📦 STEP 1: IMPORT REQUIRED LIBRARIES  
# pandas: For data manipulation (https://pandas.pydata.org/)  
# numpy: For numerical operations (https://numpy.org/)  
import pandas as pd  # pd is the standard alias for pandas  
import numpy as np   # np is the standard alias for numpy  

# 📁 STEP 2: EXTRACT DATA FROM CSV FILES  
# Load data from CSV files into pandas DataFrames  
# A DataFrame is like a table in Python  

# Load Users table with error handling  
# Use try-except to catch file errors
# Not very important right now but useful to know
try:  
    users_df = pd.read_csv('users.csv')  
except FileNotFoundError:  
    print("Error: users.csv not found. Please check the file path.")  

# Load Sales table  
sales_df = pd.read_csv('sales.csv')  

# Load Products table  
products_df = pd.read_csv('products.csv')  

# 🧪 VIEW DATA (ALWAYS CHECK YOUR DATA FIRST!)  
# .head() shows top 5 rows (default), useful for quick inspection 
print("Top 5 rows of Users table:")  
print(users_df.head())  

# .tail() shows last 5 rows  
print("\nLast 5 rows of Sales table:")  
print(sales_df.tail())  

# 📊 GET DETAILED DATA INFO  
# .info() shows column data types and missing values  
print("\nUsers table info:")  
print(users_df.info())  

# 📈 DESCRIPTIVE STATISTICS  
# .describe() gives summary statistics for numeric columns  
print("\nSales statistics:")  
print(sales_df.describe())  

# 🚨 CHECK FOR MISSING VALUES  
# .isnull() returns True for missing values, False otherwise  
# .sum() counts True values (missing values per column)  
print("\nMissing values in Users table:")  
print(users_df.isnull().sum())  

# 🔁 STEP 3: TRANSFORM DATA (CLEANING & ENRICHMENT)  

# 1️⃣ HANDLE MISSING VALUES  
# Fill missing email values with "unknown@company.com" using .fillna()  
users_df['email'] = users_df['email'].fillna("unknown@company.com")  

# Fill missing price values with median price of the same category  
# .transform() applies a function to each group  
products_df['price'] = products_df.groupby('category')['price'].transform(lambda x: x.fillna(x.median()))  

# 2️⃣ REMOVE DUPLICATES  
# Keep first occurrence of duplicate user_ids using .drop_duplicates()  
users_df = users_df.drop_duplicates(subset=['user_id'], keep='first')  

# 3️⃣ FIX INCONSISTENT DATA  
# Normalize region values (e.g., "N" → "North") using .replace()  
sales_df['region'] = sales_df['region'].replace({  
    'N': 'North',  
    'S': 'South',  
    'E': 'East',  
    'W': 'West'  
})  

# 4️⃣ CONVERT DATE COLUMNS TO DATETIME  
# Convert join_date to datetime format 
users_df['join_date'] = pd.to_datetime(users_df['join_date'])  

# Extract year and month from join_date for analysis  
users_df['join_year'] = users_df['join_date'].dt.year  
users_df['join_month'] = users_df['join_date'].dt.month_name()  

# 5️⃣ FILTER DATA  
# Keep only sales after 2024-01-01 using boolean indexing  
sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])  
recent_sales = sales_df[sales_df['sale_date'] > '2024-01-01']  

# 6️⃣ HANDLE INVALID VALUES  
# Fix negative quantities by taking absolute value
sales_df['quantity'] = sales_df['quantity'].abs()  

# 7️⃣ ADD NEW COLUMNS  
# Calculate total revenue per sale (quantity × price)  
# Merge sales with product prices first  
sales_with_price = pd.merge(sales_df, products_df[['product_id', 'price']], on='product_id', how='left')  
sales_with_price['revenue'] = sales_with_price['quantity'] * sales_with_price['price']  

# 8️⃣ APPLY CUSTOM FUNCTIONS  
# Define a function to categorize age groups 
def age_group(age):  
    if age < 18:  
        return "Under 18"  
    elif 18 <= age < 30:  
        return "18-29"  
    elif 30 <= age < 50:  
        return "30-49"  
    else:  
        return "50+"  

users_df['age_group'] = users_df['age'].apply(age_group)  

# 📥 STEP 4: LOAD CLEANED DATA TO NEW FILES  
# Save cleaned Users table to CSV using .to_csv()  
users_df.to_csv('cleaned_users.csv', index=False)  # index=False avoids saving row numbers  

# Save transformed Sales table with revenue  
sales_with_price.to_csv('cleaned_sales.csv', index=False)  

# Save filtered recent sales to Excel (requires openpyxl)  
try:  
    recent_sales.to_excel('recent_sales.xlsx', index=False)  
except ImportError:  
    print("Tip: Install openpyxl to save to Excel format.")  

# 📊 BONUS: SIMPLE ANALYSIS  
# Find top 5 products by total revenue using .groupby() and .sort_values()  
top_products = sales_with_price.groupby('product_id')['revenue'].sum().sort_values(ascending=False).head(5)  
print("\nTop 5 Products by Revenue:")  
print(top_products)  

# Calculate average revenue per user
user_revenue = sales_with_price.groupby('user_id')['revenue'].mean().rename("avg_revenue")  
print("\nAverage Revenue per User:")  
print(user_revenue.head())  

# Create a pivot table of sales by region and year
sales_pivot = pd.pivot_table(sales_with_price, values='revenue', index='region', columns=sales_with_price['sale_date'].dt.year, aggfunc=np.sum)  
print("\nSales Pivot Table by Region and Year:")  
print(sales_pivot)  

# 📈 VISUALIZE DATA (basic matplotlib integration)  
# Plot total sales by region 
import matplotlib.pyplot as plt  
region_sales = sales_with_price.groupby('region')['revenue'].sum()  
region_sales.plot(kind='bar', title='Total Sales by Region')  
plt.ylabel('Revenue')  
plt.show()  

# 📚 KEY FUNCTION EXPLANATIONS  
# ----------------------------  
# pd.read_csv(): Loads CSV data into a DataFrame 
# .head(n): Shows first n rows (default=5)  
# .tail(n): Shows last n rows (default=5)  
# .isnull().sum(): Counts missing values per column  
# .fillna(value): Replaces missing values with a specified value
# .drop_duplicates(): Removes duplicate rows
# .replace(dict): Replaces values based on dictionary mapping  
# .merge(df, on, how): Combines two DataFrames on a key column
# .groupby(column): Groups data for aggregation 
# .sort_values(ascending=False): Sorts data descending 
# .apply(function): Applies a custom function to each row/column 
# pd.pivot_table(): Creates aggregated tables for reporting 

# ✅ BEST PRACTICES  
# - Always check for missing values first 
# - Keep raw data separate from cleaned data  
# - Use meaningful variable names (e.g., sales_with_price)  
# - Add comments to explain your logic  

