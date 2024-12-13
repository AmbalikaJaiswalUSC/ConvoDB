# ConvoDB

## Installation

## Setup Instructions
   
### Prerequisites
- Python 3.7+
- MongoDB instance with access credentials. (Already given in the code)
- Required Python libraries:
  - `pymongo`
  - `pandas`
  - `nltk`
  - `random`
  - `re`
  - `mysql-connector-python`
  - `sqlalchemy`

### Setup
1. Install the required Python packages using pip:
   ```bash
   pip install pymongo pandas nltk random re
   pip install mysql-connector-python sqlalchemy
   ```

# 551 Project.py:
This Python script serves as a menu-driven interface that allows users to execute separate implementations for SQL and NoSQL functionalities. Users can choose between running SQL-related scripts, NoSQL-related scripts, or exiting the program.

# File structure:
551 Project.py- main file
NoSQL_implementation.py- for NoSQL implemnetation
sql_implementation.py- for sql implementation
Nosql_datasets - datatsets used for NoSQL implementation in the code
sql_datasets - datatsets used for SQL implementation in the code

# Features
1) SQL Implementation:
Executes the script for SQL functionalities.
Provides feedback before and after execution.

2) NoSQL Implementation:
Executes the script for NoSQL functionalities.
Provides feedback before and after execution.

3) Quit:
Exits the program gracefully.

# Run the script by executing:
```python
python 551 Project.py
```

# MongoDB and Natural Language Processing Toolkit

## Overview
This toolkit provides utilities for interacting with MongoDB databases and performing Natural Language Processing (NLP)-based query generation. It enables seamless insertion, analysis, and querying of MongoDB collections using CSV data and user-friendly natural language commands.

## Features
- **MongoDB Integration**
  - Connect to a MongoDB database.
  - Insert CSV data into MongoDB collections.
  - Analyze collections for primary and foreign keys.
- **Natural Language Querying**
  - Parse natural language input into MongoDB queries.
  - Generate queries for operations like JOIN, GROUP BY, ORDER BY, LIKE, and FIND.
- **Utility Functions**
  - Download and check required NLTK resources.
  - Classify fields into quantitative and categorical types.
  - Retrieve sample data from collections.
- **Random Query Generation**
  - Create random MongoDB queries using schema analysis.
- **List all collections**
  - View all collections available in the connected MongoDB database.
- **View collection attributes**
  - Inspect the fields and data types of a selected collection.
- **View sample data**
  - Retrieve and display sample data from a selected collection.


## Usage

### 1. Insert CSV Data into MongoDB
Call the `insert_csv_data_to_mongodb` function to load a CSV file into a MongoDB collection:
```python
insert_csv_data_to_mongodb('path/to/csv_file.csv', 'collection_name')
```

### 2. Analyze Collections
To analyze the primary and foreign keys in your MongoDB collections, use the `analyze_collections` function:
```python
primary_keys = analyze_collections()
print(primary_keys)
```

### 3. Parse Natural Language Queries
Generate MongoDB queries from natural language input:
```python
schema = get_collection_schema(db, 'collection_name')
query = parse_natural_language_input('find movies where year is 2020', schema)
print(query)
```

### 4. Generate Random Queries
Create random queries like JOIN, GROUP BY, ORDER BY, LIKE, and FIND:
```python
quantitative, categorical = classify_fields(schema)
random_queries = generate_random_queries('collection_name', quantitative, categorical, db, primary_keys)
for query in random_queries:
    print(query)
```

### 5. Execute Queries
Execute generated MongoDB queries and display results:
```python
results = execute_query(db, 'collection_name', query)
print(results)
```

## Functions Overview

### MongoDB Utilities
- `connect_to_db()`: Connects to the MongoDB database.
- `dataset_exists(collection_name)`: Checks if a collection contains data.
- `insert_csv_data_to_mongodb(csv_file_path, collection_name)`: Inserts CSV data into a collection.
- `list_collections(db)`: Lists all collections in the database.

### Collection Analysis
- `get_collection_schema(db, collection_name)`: Retrieves the schema of a collection.
- `analyze_collections()`: Identifies primary and foreign key relationships across collections.

### Query Generation
- `parse_natural_language_input(user_input, schema)`: Parses natural language input into MongoDB queries.
- `generate_random_queries(collection_name, quantitative, categorical, db, primary_keys)`: Generates random queries.

### NLP Utilities
- `download_nltk_resources()`: Downloads necessary NLTK resources.
- `classify_fields(schema)`: Classifies fields into quantitative and categorical types.

## Example Usage

### Inserting a Dataset
```python
add_dataset_option()
```
### Generating and Executing a Query
```python
schema = get_collection_schema(db, 'movies')
query = parse_natural_language_input('find movies where year is 2020', schema)
execute_query(db, 'movies', query)
```

## Future Enhancements
- Add support for advanced natural language queries.
- Enhance schema inference for more robust typing.
- Support for additional aggregation operations.

## Notes
- Ensure your MongoDB instance is accessible with the correct connection string.
- Customize connection strings, database names, and collection names as per your requirements.




# MySQL Database Interaction Script

## Overview
This script provides a comprehensive set of tools for interacting with a MySQL database. It supports functionalities like adding datasets to the database, generating random SQL queries,functions to generate SQL queries based on user input and predefined rules, and exploring the structure of tables. The script is particularly useful for database administrators, data scientists, and developers working with structured data in MySQL.

---

## Features

1. **Add Dataset to Database**
   - Allows users to upload a CSV file and create a table in the database.
   - Automatically assigns a primary key to the table.
    
2. **Database Table Exploration**
   - Lists all tables in the database.
   - Fetches column information for a specified table.
   - Retrieves sample data from tables for quick inspection.

3. **Dynamic Query Generation**
   - Generates random SQL queries including:
     - Aggregate queries (e.g., SUM, AVG, MAX, MIN).
     - SELECT queries with randomized column selection.
     - ORDER BY and GROUP BY queries with HAVING clauses.
     - JOIN queries across multiple tables.
     - LIKE queries for string pattern matching.
       
   - Generates query with language constraint and natural lagugae input.
     - Automatically selects columns for the query based on given criteria (e.g., aggregate functions).
     - Supports complex query structures like joins and groupings.
     - Can handle dynamic user queries such as `COUNT`, `SUM`, `MAX`, `MIN`, and `AVG`.

4. **NLTK Resource Management**
   - Ensures necessary NLTK resources (`punkt` and `stopwords`) are downloaded.

---

1. **Download NLTK Resources**
   The script automatically checks for required NLTK resources (`punkt` and `stopwords`) and downloads them if not present.

2. **Database Configuration**
   Update the database credentials in the `add_dataset_to_database` and `connect_to_db` functions.
   ```python
   HOST = 'your_database_host'
   DATABASE = 'your_database_name'
   USER = 'your_database_user'
   PASSWORD = 'your_database_password'
   PORT = 3306
   ```

---

## Key Functions

### 1. `add_dataset_to_database()`
Prompts the user to:
- Add a CSV file as a new table in the database.
- Assign a primary key to the table.

### 2. `download_nltk_resources()`
Ensures that required NLTK packages are available.

### 3. `connect_to_db()`
Establishes a connection to the MySQL database.

### 4. `list_tables(connection)`
Lists all tables in the connected database.

### 5. `get_table_columns(connection, table_name)`
Retrieves column information for a specified table.

### 6. `get_sample_data(connection, table_name, limit=5)`
Fetches a sample of rows from a specified table.

### 7. `sample_queries_for_table(connection, table_name)`
Generates a list of random SQL queries for a specified table, including:
- SELECT, GROUP BY, ORDER BY, and JOIN queries.
- LIKE queries for string pattern matching.

### 8. `generate_sample_query(connection, selected_table, query_keywords)`
Generates a random SQL query based on the selected table and a list of query keywords. It can generate `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `COUNT`, `DISTINCT`, and aggregation queries.

### 9. `generate_sql_query(user_query, connection, selected_table)`
Generates a SQL query by parsing and interpreting a user-provided natural language query. Supports `GROUP BY`, `ORDER BY`, `WHERE`, `LIKE`, `COUNT`, aggregate functions, and `LIMIT`.

---

## Sample Usage

### Adding a Dataset to the Database
```python
add_dataset_to_database()
```
Prompts the user to upload a CSV file, create a table, and set a primary key.

### Generating Sample Queries
```python
connection = connect_to_db()
table_name = 'your_table_name'
queries = sample_queries_for_table(connection, table_name)
for query in queries:
    print(query)
```
Prints randomly generated queries for exploration and testing.

---

## Error Handling
- The script catches and logs errors during database interactions, ensuring smooth execution.
- Common errors, like invalid column names or missing files, are gracefully handled with user prompts.

---

## Notes

- Ensure your database credentials are correct.
- Handle sensitive information (like database passwords) securely.
- The script assumes the user has appropriate permissions to modify the database.
- The `LIKE` and `WHERE` clauses are dynamically created using regex to handle user input. If you require specific pattern matching (e.g., for `LIKE`), ensure the query tokens are properly formatted.









