import mysql.connector
import random
import nltk
import re
import difflib
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import data
import pandas as pd
from sqlalchemy import create_engine, text
import logging


def add_dataset_to_database():
    try:
        # Database connection parameters
        HOST = 'sql3.freesqldatabase.com'
        DATABASE = 'sql3747009'
        USER = 'sql3747009'
        PASSWORD = 'RBJKDXRYXG'
        PORT = 3306

        # Create a connection to the MySQL database using SQLAlchemy
        connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        while True:
            # Ask user if they want to add a dataset
            add_dataset = input("Do you want to add a dataset to the database? (yes/no): ").strip().lower()

            if add_dataset in ['yes', 'y']:
                # Get dataset file path from the user
                dataset_path = input("Enter the full path of the dataset (CSV file): ").strip()
                
                # Load the dataset
                df = pd.read_csv(dataset_path)
                print(f"Dataset loaded successfully. Here are the first few rows:\n{df.head()}")

                # Ask the user to provide a table name
                table_name = input("Enter the name of the table to create in the database: ").strip()

                # Load the data into MySQL, replace existing table if it exists
                df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

                print(f"\nData loaded successfully into the table `{table_name}`.")

                # Fetch column names from the table
                with engine.connect() as connection:
                    result = connection.execute(text(f"DESCRIBE {table_name}"))
                    columns = [row['Field'] for row in result]
                
                print(f"Columns in the `{table_name}` table: {columns}")

                # Ask user to assign a primary key
                primary_key = input(f"Which column would you like to set as the primary key? Choose from {columns}: ").strip()
                if primary_key not in columns:
                    print("Invalid column name. No primary key was assigned.")
                    continue

                # Alter table to add a primary key with or without key length
                with engine.connect() as connection:
                    column_info = connection.execute(text(f"DESCRIBE {table_name}")).fetchall()
                    column_type = next((col['Type'] for col in column_info if col['Field'] == primary_key), '')

                    if 'text' in column_type or 'blob' in column_type:
                        # Use a key length for BLOB/TEXT columns (e.g., 255 characters)
                        connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key}(255));"))
                    else:
                        # Add primary key without length if the column is not TEXT/BLOB
                        connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});"))

                print(f"Primary key `{primary_key}` has been assigned successfully to the table `{table_name}`.")

            elif add_dataset in ['no', 'n']:
                print("Exiting the dataset addition process.")
                break
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")

    except Exception as e:
        print(f"An error occurred: {e}")



def download_nltk_resources():
    """Download NLTK resources only if they are not already present."""
    try:
        # Check if 'punkt' is already downloaded
        data.find('tokenizers/punkt')
    except LookupError:
        print("Downloading 'punkt'...")
        nltk.download('punkt')

    try:
        # Check if 'stopwords' is already downloaded
        data.find('corpora/stopwords')
    except LookupError:
        print("Downloading 'stopwords'...")
        nltk.download('stopwords')
        
# Call the function to download necessary resources
download_nltk_resources()

def connect_to_db():
    # Replace these values with your database connection details
    connection = mysql.connector.connect(
        host='sql3.freesqldatabase.com',  # Change if necessary
        user='sql3747009',  # Replace with your MySQL username
        password='RBJKDXRYXG',  # Replace with your MySQL password
        database='sql3747009'
    )
    return connection

def list_tables(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    cursor.close()
    return [table[0] for table in tables]

def get_table_columns(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    columns = cursor.fetchall()
    cursor.close()
    return columns

def get_sample_data(connection, table_name, limit=5):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    rows = cursor.fetchall()
    cursor.close()

    # Format the rows as per your requirement
    formatted_rows = []
    for row in rows:
        formatted_row = []
        for item in row:
            if isinstance(item, int):
                formatted_row.append(str(item))
            elif isinstance(item, float) and item.is_integer():
                formatted_row.append(str(int(item)))
            elif isinstance(item, float):
                formatted_row.append(str(item))
            else:
                formatted_row.append(f"'{item}'")
        formatted_rows.append(f"({', '.join(formatted_row)})")
    return formatted_rows

def execute_query(connection, query):
    cursor = connection.cursor(buffered=True)
    try:
        # Handle previous unread results if any
        connection.handle_unread_result()

        # Execute the query
        cursor.execute(query)

        # If the query is a SELECT, fetch results
        if query.strip().lower().startswith("select"):
            results = cursor.fetchall()
            return results
        else:
            # For non-SELECT queries, commit changes
            connection.commit()
            return None
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        cursor.close()


def categorize_columns(columns):
    numerical_columns = []
    categorical_columns = []
    
    for col in columns:
        column_name, column_type = col[0], col[1]
        if 'int' in column_type or 'float' in column_type:
            numerical_columns.append(column_name)
        elif 'varchar' in column_type or 'text' in column_type:
            categorical_columns.append(column_name)
    
    return numerical_columns, categorical_columns

def fetch_sample_data(connection, table_name, column_name):
    """Fetches a few sample values from a column in the database."""
    query = f"SELECT {column_name} FROM {table_name} LIMIT 100;"
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        values = [row[0] for row in results if row[0] is not None]  # Filter out None values
        return values
    except Exception as e:
        print(f"Error fetching sample data for column {column_name}: {e}")
        return []

def generate_random_condition_from_values(column_name, values):
    """Generates a LIKE condition based on sample values from the column."""
    if not values:
        return ""
    
    # Pick a random value from the sample and create LIKE patterns
    value = random.choice(values)
    pattern_type = random.choice(['starts_with', 'ends_with', 'contains'])

    if pattern_type == 'starts_with':
        return f"{column_name} LIKE '{value}%'"
    elif pattern_type == 'ends_with':
        return f"{column_name} LIKE '%{value}'"
    else:  # 'contains'
        return f"{column_name} LIKE '%{value}%'"

def generate_random_condition(column_name, column_type, table_name, connection):
    """Generates a random condition based on column type and sample values."""
    condition = ""
    if 'int' in column_type or 'float' in column_type:
        # For numerical columns, generate a condition with a random operator and value
        operator = random.choice(['>', '<', '=', '>=', '<='])
        value = random.randint(1, 100)  # Random value for numerical column
        condition = f"{column_name} {operator} {value}"
    
    elif 'varchar' in column_type or 'text' in column_type:
        # For string columns, generate a LIKE condition using sample data
        values = fetch_sample_data(connection, table_name, column_name)
        if values:
            condition = generate_random_condition_from_values(column_name, values)
    
    return condition

def generate_random_aggregate_query(table_name, numerical_columns):
    """Generates a random query applying an aggregate function on a numerical column"""
    if not numerical_columns:
        return None
    
    numerical_column = random.choice(numerical_columns)
    aggregate_function = random.choice(['AVG', 'SUM', 'MAX', 'MIN'])
    query = f"SELECT {aggregate_function}({numerical_column}) FROM {table_name};"
    return query


def generate_random_select_query(table_name, columns):
    """Generates a random SELECT query based on column names"""
    selected_columns = random.sample([col[0] for col in columns], random.randint(1, len(columns)))
    query = f"SELECT {', '.join(selected_columns)} FROM {table_name} LIMIT 10;"
    return query


def generate_random_order_by_query(table_name, columns):
    """Generates a random ORDER BY query based on column names"""
    column_name = random.choice([col[0] for col in columns])
    order = random.choice(['ASC', 'DESC'])
    query = f"SELECT * FROM {table_name} ORDER BY {column_name} {order} LIMIT 10;"
    return query

def generate_random_group_by_query(table_name, columns, numerical_columns):
    """
    Generates a dynamic GROUP BY query with a logical HAVING condition.
    """
    if not columns or not numerical_columns:
        return None
    
    # Dynamically choose categorical columns
    categorical_columns = [col[0] for col in columns if 'varchar' in col[1] or 'text' in col[1]]
    if not categorical_columns:
        return None
    
    # Select random columns for aggregation and grouping
    group_by_column = random.choice(categorical_columns)
    numerical_column = random.choice(numerical_columns)
    aggregate_function = random.choice(['AVG', 'SUM', 'MAX', 'MIN', 'COUNT'])
    threshold = random.randint(1, 100)  # Random whole number between 100 and 1000

    # Build SELECT part dynamically
    select_columns = [
        group_by_column,
        f"{aggregate_function}({numerical_column}) AS aggregated_value"
    ]
    select_clause = ", ".join(select_columns)

    # Build query dynamically
    query = (
        f"SELECT {select_clause} "
        f"FROM {table_name} "
        f"GROUP BY {group_by_column} "
        f"HAVING aggregated_value > {threshold};"
    )
    return query

def generate_random_join_query(table_name, columns, connection):
    """
    Generates a random JOIN query between two tables, selecting specific columns
    and prioritizing joins on matching column names.
    """
    # Fetch all available tables
    tables = list_tables(connection)
    if len(tables) <= 1:
        return None  # Not enough tables to perform a JOIN

    # Choose a different table for the JOIN
    other_table = random.choice([t for t in tables if t != table_name])
    other_table_columns = get_table_columns(connection, other_table)

    # Find columns to join on: prioritize columns with the same name
    join_candidates = [
        (col1[0], col2[0])
        for col1 in columns
        for col2 in other_table_columns
        if col1[0] == col2[0]  # Match columns with the same name
    ]

    if not join_candidates:
        # If no exact matches, fall back to columns with compatible types
        join_candidates = [
            (col1[0], col2[0])
            for col1 in columns
            for col2 in other_table_columns
            if ('int' in col1[1] or 'varchar' in col1[1]) and ('int' in col2[1] or 'varchar' in col2[1])
        ]

    if not join_candidates:
        return None  # No valid columns to join on

    # Choose a random column pair to join on
    join_column_table1, join_column_table2 = random.choice(join_candidates)

    # Select random columns from each table for the query
    table1_columns = random.sample(columns, min(2, len(columns)))  # Select up to 2 columns from table 1
    table2_columns = random.sample(other_table_columns, min(2, len(other_table_columns)))  # Select up to 2 columns from table 2

    selected_columns = [
        f"t1.{col[0]}" for col in table1_columns
    ] + [
        f"t2.{col[0]}" for col in table2_columns
    ]

    # Build the JOIN query
    query = (
        f"SELECT {', '.join(selected_columns)} "
        f"FROM {table_name} t1 "
        f"JOIN {other_table} t2 "
        f"ON t1.{join_column_table1} = t2.{join_column_table2} "
        f"LIMIT 10;"
    )
    return query

def sample_queries_for_table(connection, table_name):
    columns = get_table_columns(connection, table_name)
    numerical_columns, categorical_columns = categorize_columns(columns)

    sample_queries = []

    # Generate random aggregate queries for numerical columns
    aggregate_query = generate_random_aggregate_query(table_name, numerical_columns)
    if aggregate_query:
        sample_queries.append(aggregate_query)

    # Generate random SELECT queries
    sample_queries.append(generate_random_select_query(table_name, columns))

    # Generate random ORDER BY queries
    sample_queries.append(generate_random_order_by_query(table_name, columns))

    # Generate random GROUP BY queries
    group_by_query = generate_random_group_by_query(table_name, columns, numerical_columns)
    if group_by_query:
        sample_queries.append(group_by_query)

    # Generate random JOIN queries
    join_query = generate_random_join_query(table_name, columns,connection)
    if join_query:
        sample_queries.append(join_query)

    # Generate random LIKE queries for string columns
    for column in categorical_columns:
        condition = generate_random_condition(column, 'varchar', table_name, connection)
        sample_queries.append(f"SELECT * FROM {table_name} WHERE {condition};")

    # Generate random queries with COUNT and GROUP BY
    sample_queries.append(f"SELECT COUNT(*) FROM {table_name};")
    

    return sample_queries



def generate_sample_query(connection, selected_table, query_keywords):
    def find_primary_keys(connection, table):
        # Retrieve primary keys for the given table
        return [col[0] for col in get_table_columns(connection, table) if col[2] == 'PRI']  # Assuming col[2] marks the key type.

    def find_foreign_keys(connection, table):
        # Retrieve foreign keys for the given table
        return [col[0] for col in get_table_columns(connection, table) if col[2] == 'FK']  # Assuming col[2] marks the key type.

    def find_matching_columns(columns1, columns2):
        # Return a list of columns with similar types
        return [(col1[0], col2[0]) for col1 in columns1 for col2 in columns2 if col1[1] == col2[1]]
    
    def select_random_columns(columns, exclude=[], count=2):
        unique_columns = [col for col in columns if col not in exclude]
        return random.sample(unique_columns, min(len(unique_columns), count))

    # Fetch table columns
    columns = get_table_columns(connection, selected_table)
    numerical_columns = [col[0] for col in columns if 'int' in col[1].lower() or 'float' in col[1].lower()]
    categorical_columns = [col[0] for col in columns if 'varchar' in col[1].lower() or 'text' in col[1].lower()]

    query = f"SELECT * FROM {selected_table}"

    join_table, join_column_1, join_column_2 = None, None, None

    # Handle JOIN
    if 'join' in query_keywords:
        tables = list_tables(connection)
        if len(tables) < 2:
            return "Not enough tables to perform a join."

        join_table = random.choice([t for t in tables if t != selected_table])

        # Fetch columns for both tables
        join_columns = get_table_columns(connection, join_table)

        # Try to use primary or foreign keys for the join
        primary_keys1 = find_primary_keys(connection, selected_table)
        primary_keys2 = find_primary_keys(connection, join_table)
        foreign_keys1 = find_foreign_keys(connection, selected_table)
        foreign_keys2 = find_foreign_keys(connection, join_table)

        # Join on primary key if available
        if primary_keys1 and primary_keys2:
            join_column_1 = primary_keys1[0]
            join_column_2 = primary_keys2[0]
        # Join on foreign key if available
        elif foreign_keys1 and primary_keys2:
            join_column_1 = foreign_keys1[0]
            join_column_2 = primary_keys2[0]
        elif foreign_keys2 and primary_keys1:
            join_column_1 = primary_keys1[0]
            join_column_2 = foreign_keys2[0]
        # Otherwise, join on matching column types
        else:
            matching_columns = find_matching_columns(columns, join_columns)
            if matching_columns:
                join_column_1, join_column_2 = matching_columns[0]
            else:
                join_column_1 = random.choice(columns)[0]
                join_column_2 = random.choice(join_columns)[0]

        # Select random columns to display
        selected_columns_base = select_random_columns([col[0] for col in columns], count=2)
        selected_columns_join = select_random_columns([col[0] for col in join_columns], count=2)

        selected_columns = (
            [f"t1.{col}" for col in selected_columns_base]
            + [f"t2.{col}" for col in selected_columns_join]
        )

        query = f"SELECT {', '.join(selected_columns)} FROM {selected_table} t1 JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2}"

    # Handle GROUP BY
    if 'group by' in query_keywords:
        column_group_by = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
        if 'join' in query_keywords:
            query = f"SELECT t1.{column_group_by}, COUNT(*) FROM {selected_table} t1 JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2} GROUP BY t1.{column_group_by}"
        else:
            query = f"SELECT {column_group_by}, COUNT(*) FROM {selected_table} GROUP BY {column_group_by}"

        # Handle GROUP BY + HAVING
        if 'having' in query_keywords:
            having_condition = f"COUNT(*) > {random.randint(1, 5)}"
            query += f" HAVING {having_condition}"

    if 'order by' in query_keywords:
        order_by_column = random.choice(columns)[0]
        order = random.choice(['ASC', 'DESC'])
        if 'group by' in query_keywords:
            group_by_column = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
            query = f"SELECT {group_by_column}, COUNT(*) FROM {selected_table} GROUP BY {group_by_column} ORDER BY {order_by_column} {order}"
        elif 'count' in query_keywords:
            count_column = random.choice(numerical_columns) if numerical_columns else random.choice(columns)[0]
            query = f"SELECT COUNT({count_column}) FROM {selected_table} ORDER BY {count_column} {order}"
        else:
            query = f"SELECT * FROM {selected_table} ORDER BY {order_by_column} {order}"


    # Handle DISTINCT
    if 'distinct' in query_keywords and 'group by' not in query_keywords:
        column_distinct = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
        query = f"SELECT DISTINCT {column_distinct} FROM {selected_table}"

    # Handle COUNT
    if 'count' in query_keywords:
        column_count = random.choice(categorical_columns) if categorical_columns else random.choice(numerical_columns)
        if 'distinct' in query_keywords:
            query = f"SELECT COUNT(DISTINCT {column_count}) FROM {selected_table}"
        elif 'group by' in query_keywords:
            query = f"SELECT {column_group_by}, COUNT(*) FROM {selected_table} GROUP BY {column_group_by}"
        else:
            query = f"SELECT COUNT({column_count}) FROM {selected_table}"

    # Handle aggregate functions
    if any(agg in query_keywords for agg in ['aggregate', 'sum', 'max', 'min', 'avg']):
        column_agg = random.choice(numerical_columns) if numerical_columns else random.choice(columns)[0]
        if 'group by' in query_keywords:
            query = f"SELECT {column_group_by}, MAX({column_agg}), MIN({column_agg}), AVG({column_agg}), SUM({column_agg}) FROM {selected_table} GROUP BY {column_group_by}"
        else:
            query = f"SELECT MAX({column_agg}), MIN({column_agg}), AVG({column_agg}), SUM({column_agg}) FROM {selected_table}"
    
    if 'like' in query_keywords:
        like_column = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
        
        # Retrieve a random value from the column to use as the LIKE pattern
        cursor = connection.cursor()
        cursor.execute(f"SELECT DISTINCT {like_column} FROM {selected_table} WHERE {like_column} IS NOT NULL LIMIT 10")
        results = cursor.fetchall()
        cursor.close()

        if results:
            like_value = random.choice(results)[0]
            like_pattern = f"'%{like_value}%'"
        else:
            like_pattern = f"'%{random.choice(['test', 'example', 'sample'])}%'"

        if 'count' in query_keywords:
            query = f"SELECT COUNT(*) FROM {selected_table} WHERE {like_column} LIKE {like_pattern}"
        elif 'join' in query_keywords:
            if join_table:
                query = (
                    f"SELECT t1.*, t2.* "
                    f"FROM {selected_table} t1 "
                    f"JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2} "
                    f"WHERE t1.{like_column} LIKE {like_pattern}"
                )
            else:
                query = f"SELECT * FROM {selected_table} WHERE {like_column} LIKE {like_pattern}"
        else:
            query = f"SELECT * FROM {selected_table} WHERE {like_column} LIKE {like_pattern}"


    # JOIN + ORDER BY + GROUP BY
    elif 'join' in query_keywords and 'group by' in query_keywords and 'order by' in query_keywords:
        group_by_column = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
        order_by_column = random.choice(columns)[0]
        order = random.choice(['ASC', 'DESC'])
        query = (
            f"SELECT t1.{group_by_column}, COUNT(*) "
            f"FROM {selected_table} t1 "
            f"JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2} "
            f"GROUP BY t1.{group_by_column} "
            f"ORDER BY t1.{order_by_column} {order}"
        )

    # JOIN + COUNT + GROUP BY
    if 'join' in query_keywords and 'count' in query_keywords and 'group by' in query_keywords:
        group_by_column = random.choice(categorical_columns) if categorical_columns else random.choice(columns)[0]
        selected_columns_base = select_random_columns([col[0] for col in columns], count=2)
        selected_columns_join = select_random_columns([col[0] for col in get_table_columns(connection, join_table)], count=2)

        selected_columns = (
            [f"t1.{col}" for col in selected_columns_base] + 
            [f"t2.{col}" for col in selected_columns_join]
        )
        
        query = (
            f"SELECT {', '.join(selected_columns)}, COUNT(*) AS total_count "
            f"FROM {selected_table} t1 "
            f"JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2} "
            f"GROUP BY t1.{group_by_column}"
        )

    # JOIN + AGGREGATE
    elif 'join' in query_keywords and any(agg in query_keywords for agg in ['aggregate', 'sum', 'max', 'min', 'avg']):
        aggregate_column = random.choice(numerical_columns) if numerical_columns else random.choice(columns)[0]
        query = (
            f"SELECT MAX(t1.{aggregate_column}) AS max_value, "
            f"MIN(t1.{aggregate_column}) AS min_value, "
            f"AVG(t1.{aggregate_column}) AS avg_value, "
            f"SUM(t1.{aggregate_column}) AS sum_value "
            f"FROM {selected_table} t1 "
            f"JOIN {join_table} t2 ON t1.{join_column_1} = t2.{join_column_2}"
        )

    return query

def generate_sql_query(user_query, connection, selected_table):
    # Tokenize the user query
    query_tokens = word_tokenize(user_query.lower())
    
    # Define mappings for aggregate functions
    aggregate_map = {
        "sum": "SUM",
        "average": "AVG",
        "avg": "AVG",
        "maximum": "MAX",
        "highest": "MAX",
        "max": "MAX",
        "minimum": "MIN",
        "lowest": "MIN",
        "min": "MIN",
        "count": "COUNT",
    }

    aggregation_columns = []  # Holds SQL aggregation expressions
    selected_columns = []  # Columns explicitly mentioned in the user query
    group_by_columns = []  # Holds columns for GROUP BY
    order_by_column = None
    order_direction = None  # Optional order direction
    limit = None
    where_clause = ""

    # Fetch columns from the selected table in the database
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {selected_table}")
    columns = cursor.fetchall()

    # Extract column names
    column_names = [col[0].lower() for col in columns]  # Lowercase for matching
    col_n=column_names
    # print("column names",column_names)

    # --- Identify aggregate functions and regular columns in user query ---
    for i, token in enumerate(query_tokens):
        # print("token",token)
        if token in aggregate_map:
            # Check if the next token is a column name
            if i + 1 < len(query_tokens) and query_tokens[i + 1] in column_names:
                column = query_tokens[i + 1]
                aggregation_columns.append(f"{aggregate_map[token]}({column})")
                col_n.remove(column)
        elif token in col_n:
            # Add non-aggregate columns explicitly mentioned in the query
            selected_columns.append(token)
    # print("selected columns,",selected_columns)
    # print("aggregate columns,",aggregation_columns)

    # --- Regex to find "group by" and columns after it ---
    group_by_pattern = r'\bgroup\sby\b\s+([a-zA-Z0-9_,\s]+\bwhere\b)'  # Regex for 'GROUP BY'
    match_group_by = re.search(group_by_pattern, user_query.lower())
    # print(match_group_by)
    if not match_group_by:
        group_by_pattern = r'\bgroup\sby\b\s+([a-zA-Z0-9_,\s]+)'  # Regex for 'GROUP BY'
        match_group_by = re.search(group_by_pattern, user_query.lower())
        # print(match_group_by)
    expanded_group_by_columns = []
    if match_group_by:
        group_by_columns = [col.strip() for col in match_group_by.group(1).split(',')]
        for item in group_by_columns:
            # Use regex to find individual words and keywords like 'where' or 'like'
            expanded_group_by_columns.extend(re.findall(r'\b(?:where|like|\w+)\b', item.lower()))
        group_by_columns= expanded_group_by_columns 
        group_by_columns = [col for col in group_by_columns if col in column_names]

    # --- Regex to find "order by" and column(s) with optional direction ---
    order_by_pattern = r'\border\sby\b\s+([a-zA-Z0-9_,\s]+)(\s+(asc|desc))?'  # Regex for 'ORDER BY'
    match_order_by = re.search(order_by_pattern, user_query.lower())
    if match_order_by:
        order_by_column = [col.strip() for col in match_order_by.group(1).split(',')]
        if match_order_by.group(3):  # Check for ASC/DESC
            order_direction = match_order_by.group(3).upper()

    # --- Regex to find "limit" clause ---
    limit_pattern = r'\blimit\s+(\d+)'  # Regex for 'LIMIT'
    match_limit = re.search(limit_pattern, user_query.lower())
    if match_limit:
        limit = match_limit.group(1)

    # --- Regex to find "where" clause ---
    where_pattern = r'\bwhere\b\s+(.*)'  # Regex for 'WHERE'
    match_where = re.search(where_pattern, user_query.lower())
    if match_where:
        where_clause = f"WHERE {match_where.group(1)}"

     # --- Handle LIKE clauses in WHERE ---
    like_pattern = r'(\w+)\s+like\s+\'(.*?)\''  # Regex for LIKE clauses (e.g., column LIKE 'pattern')
    like_matches = re.findall(like_pattern, user_query.lower())
    for column, pattern in like_matches:
        if column in column_names:
            # where_clause += f" AND {column} LIKE '{pattern}'"
            where_clause = re.sub(like_pattern, rf"{column} like '%\2%'", where_clause)
            if column not in selected_columns:
                selected_columns.append(column)  # Ensure the column used in LIKE is in SELECT


    # --- Build the SELECT clause ---
    # If aggregation columns are present, use them. Otherwise, use selected_columns.
    cols_final = selected_columns + aggregation_columns 
    my_set = set(cols_final)
    cols_final= list(my_set)
    
    if cols_final:
        select_clause = ", ".join(cols_final)
    else:
        # If no columns are explicitly mentioned, include all columns
        select_clause = ", ".join([col.capitalize() for col in column_names])

    # --- Build the GROUP BY clause ---
    group_by_clause = ""
    if group_by_columns:
        group_by_clause = f"GROUP BY {', '.join(group_by_columns)}"

    # --- Build the ORDER BY clause ---
    order_by_clause = ""
    if order_by_column:
        order_by_clause = f"ORDER BY {', '.join(order_by_column)}"
        if order_direction:
            order_by_clause += f" {order_direction}"

    # --- Build the LIMIT clause ---
    limit_clause = f"LIMIT {limit}" if limit else ""

    # --- Construct the final SQL query ---
    sql_query = f"SELECT {select_clause} FROM {selected_table} {where_clause} {group_by_clause} {order_by_clause}"

    return sql_query


def menu():
    print("\n=== Database Explorer Menu ===")
    print("1. List all tables in the database")
    print("2. View columns (attributes) of a table")
    print("3. View sample data from a table")
    print("4. View sample queries for a table")
    print("5. Generate sample SQL query for specific operations")
    print("6. Generate SQL query based on natural language")
    print("7. Execute a custom query")
    print("q. Quit")
    return input("Select an option: ")

def get_table_columns_with_constraints(connection, table_name):
    """
    Fetch column details, including type, primary key status, and nullability, for a given table.
    """
    query = f"""
    SELECT 
        COLUMN_NAME, 
        COLUMN_TYPE, 
        IS_NULLABLE, 
        COLUMN_KEY 
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = DATABASE();
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

def main():
    connection = connect_to_db()
    add_dataset_to_database()

    while True:
        choice = menu()

        if choice == '1':
            # List all tables
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")
        
        elif choice == '2':
            # View columns (attributes) of a selected table
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")

            table_choice = input("Select a table by number to view its attributes: ")

            try:
                table_index = int(table_choice) - 1
                if 0 <= table_index < len(tables):
                    selected_table = tables[table_index]
                    columns = get_table_columns(connection, selected_table)
                    print(f"\nColumns of {selected_table}:")
                    for col in columns:
                        print(f"Column: {col[0]}, Type: {col[1]}")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        
        elif choice == '3':
            # View sample data from a selected table
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")

            table_choice = input("Select a table by number to view its sample data: ")

            try:
                table_index = int(table_choice) - 1
                if 0 <= table_index < len(tables):
                    selected_table = tables[table_index]
                    sample_data = get_sample_data(connection, selected_table)
                    
                    # Retrieve column names for the selected table
                    columns = get_table_columns(connection, selected_table)
                    column_names = [col[0] for col in columns]
                    
                    # Print the column names with separator
                    print(f"\nSample data from {selected_table}:")
                    print(" | ".join(column_names))  # Only column names with separator
                    
                    # Print the sample data rows without separator
                    for row in sample_data:
                        print(row)  # Print each row as is, without separating the data values

                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")


        
        elif choice == '4':
            # View sample queries for a selected table
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")

            table_choice = input("Select a table by number to view its sample queries: ")

            try:
                table_index = int(table_choice) - 1
                if 0 <= table_index < len(tables):
                    selected_table = tables[table_index]
                    print(f"\nSample queries for {selected_table}:")
                    queries = sample_queries_for_table(connection, selected_table)
                    for idx, query in enumerate(queries, start=1):
                        print(f"{idx}. {query}")

                    query_choice = input("\nSelect a query to execute by number, or 'b' to go back: ")
                    if query_choice.lower() == 'b':
                        continue
                    query_index = int(query_choice) - 1
                    if 0 <= query_index < len(queries):
                        query = queries[query_index]
                        results = execute_query(connection, query)
                        if results is not None:
                            print("Query Results:")
                            for row in results:
                                print(row)
                        else:
                            print("Query executed successfully.")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        
        elif choice == '5':
            # Generate sample SQL query for a specific operation based on user input
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")

            table_choice = input("Select a table by number to generate a sample query: ")

            try:
                table_index = int(table_choice) - 1
                if 0 <= table_index < len(tables):
                    selected_table = tables[table_index]
                    query_keywords = input("Enter SQL keywords (e.g., count, distinct, group by, order by, join): ").strip()
                    sample_query = generate_sample_query(connection, selected_table, query_keywords)

                    print(f"\nSample query for '{query_keywords}':\n{sample_query}")

                    execute_choice = input("\nWould you like to execute this query? (yes/no): ").strip().lower()
                    if execute_choice in ['yes', 'y']:
                        try:
                            limit_choice = input("Do you want to set a LIMIT? Enter a number or type 'no' to see all rows: ").strip()
                            
                            # Determine the query with or without a LIMIT clause
                            if limit_choice.isdigit():
                                limit_value = int(limit_choice)
                                sample_query_with_limit = f"{sample_query} LIMIT {limit_value}"
                            elif limit_choice.lower() in ['no', 'n']:
                                sample_query_with_limit = sample_query
                            else:
                                print("Invalid input. Showing all rows by default.")
                                sample_query_with_limit = sample_query

                            # Execute query
                            cursor = connection.cursor(buffered=True)
                            cursor.execute(sample_query_with_limit)

                            # Fetch the column names and rows
                            column_names = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()

                            # Display query and results
                            print(f"\nExecuting query:\n{sample_query_with_limit}")
                            print("\nQuery Results:")

                            # Print column headers
                            print(" | ".join(column_names))  # Print column names
                            print("-" * (len(column_names) * 15))  # Print separator line

                            # Print each row under corresponding column headers
                            for row in rows:
                                print(" | ".join(str(value) if value is not None else "NULL" for value in row))

                        except Exception as e:
                            print(f"Error executing query: {e}")

                        finally:
                            cursor.close()  # Ensure the cursor is closed
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '6':
            # Generate sample SQL query for a specific operation based on user input
            tables = list_tables(connection)
            print("\nAvailable tables:")
            for idx, table in enumerate(tables, start=1):
                print(f"{idx}. {table}")

            table_choice = input("Select a table by number to generate query: ")

            try:
                table_index = int(table_choice) - 1
                if 0 <= table_index < len(tables):
                    selected_table = tables[table_index]
                    user_query = input("\nEnter your natural language query: ")
                    sample_query = generate_sql_query(user_query, connection, selected_table)

                    print(f"\nQuery generated: \n {sample_query}")

                    execute_choice = input("\nWould you like to execute this query? (yes/no): ").strip().lower()
                    if execute_choice in ['yes', 'y']:
                        try:
                            limit_choice = input("Do you want to set a LIMIT? Enter a number or type 'no' to see all rows: ").strip()
                            
                            # Determine the query with or without a LIMIT clause
                            if limit_choice.isdigit():
                                limit_value = int(limit_choice)
                                sample_query_with_limit = f"{sample_query} LIMIT {limit_value}"
                            elif limit_choice.lower() in ['no', 'n']:
                                sample_query_with_limit = sample_query
                            else:
                                print("Invalid input. Showing all rows by default.")
                                sample_query_with_limit = sample_query

                            # Execute query
                            cursor = connection.cursor(buffered=True)
                            cursor.execute(sample_query_with_limit)

                            # Fetch the column names and rows
                            column_names = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()

                            # Display query and results
                            print(f"\nExecuting query:\n{sample_query_with_limit}")
                            print("\nQuery Results:")

                            # Print column headers
                            print(" | ".join(column_names))  # Print column names
                            print("-" * (len(column_names) * 15))  # Print separator line

                            # Print each row under corresponding column headers
                            for row in rows:
                                print(" | ".join(str(value) if value is not None else "NULL" for value in row))

                        except Exception as e:
                            print(f"Error executing query: {e}")

                        finally:
                            cursor.close()  # Ensure the cursor is closed
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")
                
        elif choice == '7':
            # Execute a custom query
            custom_query = input("Enter your SQL query: ")

            results = execute_query(connection, custom_query)

            if results is not None:
                try:
                    # Fetch column names using a new cursor
                    cursor = connection.cursor(buffered=True)
                    cursor.execute(custom_query)
                    column_names = [desc[0] for desc in cursor.description]

                    # Display column headers
                    print("Query Results:")
                    print(" | ".join(column_names))  # Print column names as headers
                    print("-" * (len(column_names) * 15))  # Separator line

                    # Display each row under the appropriate column header
                    for row in results:
                        print(" | ".join(str(value) if value is not None else "NULL" for value in row))

                except Exception as e:
                    print(f"Error retrieving column names: {e}")
                finally:
                    cursor.close()  # Ensure the cursor is closed
            else:
                print("Query executed successfully.")

        elif choice.lower() == 'q':
            print("Exiting...")
            break
        
        else:
            print("Invalid option. Please select a valid menu option.")

    connection.close()

if __name__ == "__main__":
    main()
