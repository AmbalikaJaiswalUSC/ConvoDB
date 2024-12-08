import re
import pymongo
import pandas as pd
import random
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from itertools import combinations
from nltk import data

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

# Calling the function to download necessary resources
download_nltk_resources()

def connect_to_db():
    #MongoDB connection details
    client = pymongo.MongoClient("mongodb+srv://spmehta:DSCI551@nosql-db.xamqe.mongodb.net/")  #MongoDB connection string
    db = client['NoSQL']  #database name
    return db

def dataset_exists(collection_name):
    """Check if the collection already contains data."""
    db = connect_to_db()
    collection = db[collection_name]
    return collection.count_documents({}) > 0

def insert_csv_data_to_mongodb(csv_file_path, collection_name):
    """Inserts data from a CSV file into the specified MongoDB collection."""
    
    # Loading CSV file into a pandas DataFrame
    data = pd.read_csv(csv_file_path)
    
    # Filling missing values with an empty string (if any)
    data = data.fillna(" ")
    
    # Converting DataFrame to a list of dictionaries (records)
    data_dict = data.to_dict(orient='records')
    
    # Checkning if the collection already contains data
    if dataset_exists(collection_name):
        print(f"Dataset already exists in the '{collection_name}' collection. Skipping insertion.")
    else:
        # Inserting the data into the MongoDB collection
        db = connect_to_db()
        collection = db[collection_name]
        collection.insert_many(data_dict)
        print(f"CSV data from {csv_file_path} has been successfully added to the '{collection_name}' collection!")

def add_dataset_option():
    """Prompt the user to add a dataset and handle the insertion."""
    user_input = input("Do you want to add a dataset? (y/n): ").strip().lower()
    
    if user_input == 'y':
        csv_file_path = input("Enter the full path of the CSV file: ").strip()
        collection_name = input("Enter the collection name to insert data into: ").strip()

        try:
            insert_csv_data_to_mongodb(csv_file_path, collection_name)
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("No dataset added.")

def list_collections(db):
    # Listing all collections in the database
    return db.list_collection_names()

def get_collection_schema(db, collection_name):
    # Infering types from sample documents
    collection = db[collection_name]
    sample_document = collection.find_one()
    if sample_document:
        schema = {key: type(value).__name__ for key, value in sample_document.items()}
        # Explicitly check and correct the 'year' type if the year is given in int convert it to string.
        if "year" in schema and schema["year"] == "int":
            schema["year"] = "str"  # Reflect the intended type in the schema
        if "Year" in schema and schema["Year"] == "int":
            schema["Year"] = "str"  # Reflect the intended type in the schema

        return schema
    else:
        return None

def get_sample_data(db, collection_name, limit=5):
    # Get a sample of documents from the collection
    collection = db[collection_name]
    sample_data = list(collection.find().limit(limit))
    df = pd.DataFrame(sample_data)
    return df

def classify_fields(schema):
    """Classify fields into quantitative (numerical) and categorical (non-numerical)"""
    quantitative = []
    categorical = []
    
    for field, field_type in schema.items():
        if field == "_id":
            continue  # Skip the default MongoDB '_id' field
        if field_type in ['int', 'float', 'int64', 'float64']:
            quantitative.append(field)
        else:
            categorical.append(field)

    return quantitative, categorical


def parse_natural_language_input(user_input, schema):
    """
    Parse user natural language input and generate an executable MongoDB query.

    Parameters:
    - user_input: Natural language input string from the user.
    - schema: Schema of the collection (dictionary with field names and types).

    Returns:
    - MongoDB query (list for aggregate or dict for find).
    """

    # Tokenize and lowercase input
    tokens = user_input.lower().split()

    # Map schema fields to lowercase for matching
    schema_map = {key.lower(): key for key in schema.keys()}

    # Identify fields and keywords
    target_field = next((schema_map[token] for token in tokens if token in schema_map), None)
    group_by_field = None
    sort_field = None
    sort_order = None
    aggregation_type = None
    distinct_flag = "distinct" in tokens
    like_value = None
    like_field = None
    match_condition = None

    # Detect "group by" or "by"
    if "group by" in user_input or "by" in user_input:
        match = re.search(r"(group by|by) (\w+)", user_input, re.IGNORECASE)
        if match:
            group_by_field_token = match.group(2).lower()
            group_by_field = schema_map.get(group_by_field_token)

    # Detect aggregation types like "highest", "lowest", "average"
    if "highest" in tokens or "max" in tokens or "maximum" in tokens:
        aggregation_type = "$max"
    elif "lowest" in tokens or "min" in tokens or "minimum" in tokens:
        aggregation_type = "$min"
    elif "average" in tokens or "mean" in tokens:
        aggregation_type = "$avg"

    # Detect "order by" or "sort by" and determine sort order
    if "order by" in user_input or "sort by" in user_input:
        sort_match = re.search(r"(order by|sort by) (\w+)", user_input, re.IGNORECASE)
        if sort_match:
            sort_field_token = sort_match.group(2).lower()
            sort_field = schema_map.get(sort_field_token)
            sort_order = 1 if "ascending" in tokens or "asc" in tokens else -1

    # Detect "like" for pattern matching
    if "like" in tokens:
        like_match = re.search(r"(\w+) like (.+)", user_input, re.IGNORECASE)
        if like_match:
            like_field_token = like_match.group(1).lower()
            like_field = schema_map.get(like_field_token)
            like_value = like_match.group(2).strip()

    # Detect "find" or "where" conditions
    if "find" in tokens or "search" in tokens or "where" in user_input:
        match = re.search(r"where (\w+) is (.+)", user_input, re.IGNORECASE)
        if match:
            field_token = match.group(1).lower()
            value = match.group(2).strip()
            field = schema_map.get(field_token)
            if field:
                if schema[field] == "string":
                    value = value.strip()  # Keep as string
                else:
                    try:
                        # Try converting to int or float if applicable
                        value = int(value) if value.isdigit() else float(value)
                    except ValueError:
                        pass  # Keep as a string if conversion fails
                match_condition = {field: value}

    # Build the query dynamically
    query = []

    # Add match stage for "where" or "like"
    if match_condition:
        query.append({"$match": match_condition})
    elif like_value and like_field:
        query.append({"$match": {like_field: {"$regex": like_value, "$options": "i"}}})

    # Add aggregation stage
    if group_by_field:
        group_stage = {
            "_id": f"${group_by_field}"
        }
        # Add an aggregation operation if specified
        if aggregation_type and target_field:
            group_stage[f"{aggregation_type.replace('$', '')}_value"] = {aggregation_type: f"${target_field}"}
        query.append({"$group": group_stage})

    # Add sorting stage
    if sort_field and sort_order is not None:
        query.append({"$sort": {sort_field: sort_order}})

    # Add distinct stage
    if distinct_flag and target_field:
        query.append({"$group": {"_id": f"${target_field}"}})

    # Return the built query
    return query if query else "-- Could not parse the input into a valid query. Please provide more details."


def get_random_value_from_category(db, collection_name, category_field):
    """
    Fetch a random value from a specific field within a MongoDB collection.
    Ensures the field exists and is not null in the selected document.

    Used to get values to input in the randomly generated queries with specific laguage constraint.
    """
    pipeline = [
        {"$match": {category_field: {"$exists": True, "$ne": None}}},  # Exclude documents where the field is null or missing
        {"$sample": {"size": 1}},  # Randomly select one document
        {"$project": {category_field: 1, "_id": 0}}  # Project only the desired field
    ]
    result = list(db[collection_name].aggregate(pipeline))
    if result:
        return result[0].get(category_field)
    return None


def analyze_collections():
    """
    Analyzes collections in a MongoDB database to identify primary and foreign keys,
    excluding the `_id` field.

    Returns:
        primary_keys (dict): Dictionary with primary and foreign key relationships.
    """
    # Connect to the MongoDB database
    db = connect_to_db()

    primary_keys = {}

    # List all collections
    collections = db.list_collection_names()

    # Identify primary keys
    for collection_name in collections:
        collection = db[collection_name]
        schema = collection.find_one()

        # Skip if collection is empty
        if not schema:
            continue

        # Extract field names, excluding '_id'
        fields = [field for field in schema.keys() if field != "_id"]

        # Detect potential primary keys
        unique_fields = []
        for field in fields:
            # Check for uniqueness of the field (excluding _id)
            unique_count = collection.aggregate([
                {"$group": {"_id": f"${field}"}},
                {"$count": "unique_values"}
            ])
            total_count = collection.count_documents({})
            unique_count = next(unique_count, {}).get("unique_values", 0)

            if unique_count == total_count:
                unique_fields.append(field)

        # Handle composite primary keys
        if not unique_fields:
            for r in range(2, len(fields) + 1):
                for combination in combinations(fields, r):
                    group_by_fields = {f: f"${f}" for f in combination}
                    unique_count = collection.aggregate([
                        {"$group": {"_id": group_by_fields}},
                        {"$count": "unique_combinations"}
                    ])
                    unique_count = next(unique_count, {}).get("unique_combinations", 0)

                    if unique_count == total_count:
                        unique_fields = list(combination)
                        break
                if unique_fields:
                    break

        primary_keys[collection_name] = {
            "primary_key": unique_fields,
            "related_collections": {}
        }

    # Identify foreign key relationships
    for collection_name in collections:
        for related_collection in collections:
            if collection_name == related_collection:
                continue

            collection = db[collection_name]
            related_collection_data = db[related_collection]

            common_fields = set(primary_keys[collection_name]["primary_key"]).intersection(
                related_collection_data.find_one().keys()
            )

            if common_fields:
                primary_keys[collection_name]["related_collections"][related_collection] = list(common_fields)

    return primary_keys


# # Display the primary and foreign keys
# primary_keys = analyze_collections()

# for collection, details in primary_keys.items():
#     print(f"Collection: {collection}")
#     print(f"  Primary Key: {details['primary_key']}")
#     print(f"  Foreign Keys: {details['related_collections']}")


def generate_random_queries(collection_name, quantitative, categorical, db, primary_keys):
    """
    Generate a list of 5 complete MongoDB queries: JOIN, GROUP BY, ORDER BY, LIKE, and FIND.

    Parameters:
    - collection_name: Name of the MongoDB collection.
    - quantitative: List of quantitative fields.
    - categorical: List of categorical fields.
    - db: MongoDB database connection object.
    - primary_keys: Dictionary defining primary and foreign key relationships.

    Returns:
    - List of complete MongoDB queries as strings.
    """
    queries = []

    # 1. JOIN query
    if collection_name in primary_keys:
        primary_key = primary_keys[collection_name]["primary_key"]
        related_collections = primary_keys[collection_name]["related_collections"]

        for related_collection, foreign_keys in related_collections.items():
            if isinstance(primary_key, list) and isinstance(foreign_keys, list) and len(primary_key) == len(foreign_keys):
                # Composite key JOIN using `$lookup` and `$expr`
                match_conditions = [
                    {"$eq": [f"${local_field}", f"$$foreign_{foreign_field}"]}
                    for local_field, foreign_field in zip(primary_key, foreign_keys)
                ]
                join_pipeline = [
                    {
                        "$lookup": {
                            "from": related_collection,
                            "let": {f"foreign_{foreign_field}": f"${local_field}"
                                    for local_field, foreign_field in zip(primary_key, foreign_keys)},
                            "pipeline": [
                                {"$match": {"$expr": {"$and": match_conditions}}}
                            ],
                            "as": "joined_data"
                        }
                    },
                    {"$unwind": {"path": "$joined_data", "preserveNullAndEmptyArrays": True}}
                ]
                queries.append(f"db.{collection_name}.aggregate({join_pipeline})")
                break  # Generate only one JOIN query

    # 2. GROUP BY query
    if quantitative and categorical:
        numeric_field = random.choice(quantitative)
        category_field = random.choice(categorical)
        group_pipeline = [
            {"$group": {"_id": f"${category_field}", "average": {"$avg": f"${numeric_field}"}}}
        ]
        queries.append(f"db.{collection_name}.aggregate({group_pipeline})")

    # 3. ORDER BY query
    if quantitative:
        numeric_field = random.choice(quantitative)
        order = random.choice([1, -1])  # MongoDB uses 1 for ASC and -1 for DESC
        sort_pipeline = [{"$sort": {numeric_field: order}}]
        queries.append(f"db.{collection_name}.aggregate({sort_pipeline})")

    # 4. LIKE query
    if categorical:
        category_field = random.choice(categorical)
        random_value = get_random_value_from_category(db, collection_name, category_field)
        if random_value:
            like_query = {"$match": {category_field: {"$regex": random_value, "$options": "i"}}}
            queries.append(f"db.{collection_name}.find({like_query})")
        else:
            queries.append(f"-- Could not generate LIKE query for {category_field} due to lack of data")

    # 5. FIND query
    if categorical and quantitative:
        category_field = random.choice(categorical)
        numeric_field = random.choice(quantitative)
        find_query = {
            "$and": [
                {category_field: {"$exists": True}},
                {numeric_field: {"$exists": True}}
            ]
        }
        queries.append(f"db.{collection_name}.find({find_query})")

    return queries




def execute_query(db, collection_name, query):
    """
    Execute the generated MongoDB query and display the results.
    """
    try:
        results = list(db[collection_name].aggregate(query))  # MongoDB aggregation pipeline format
        if results:
            df = pd.DataFrame(results)
            print("1. Display Whole data")
            print("2. Dsiplay First 10 rows")
            u_i = input("Select an option: ")
            if u_i == "1":
                print("\nQuery Results:")
                print(df)
            else:
                print("\nQuery Results:")
                print(df.head(10))  # Display first 10 rows
            
        else:
            print("No results found.")
    except Exception as e:
        print("Error executing query:", e)


def generate_custom_query(collection_name, quantitative, categorical, user_input, db):
    """Generate a sample query based on the user-specified construct, including combined cases."""
    import random

    queries = []

    # Flags to track included constructs
    include_group_by = "group by" in user_input.lower()
    include_like = "like" in user_input.lower()
    include_order_by = "order by" in user_input.lower()
    include_join = "join" in user_input.lower()

    # Initialize the base query pipeline
    base_pipeline = []

    # Handle JOIN if specified
    if include_join:
        if collection_name in primary_keys:
            primary_key = primary_keys[collection_name]["primary_key"]
            related_collections = primary_keys[collection_name]["related_collections"]

            for related_collection, foreign_keys in related_collections.items():
                if isinstance(primary_key, list) and isinstance(foreign_keys, list) and len(primary_key) == len(foreign_keys):
                    # For multiple key joins, use `$lookup` with `$expr`
                    match_conditions = [{"$eq": [f"${local_field}", f"$$foreign_{foreign_field}"]}
                                        for local_field, foreign_field in zip(primary_key, foreign_keys)]

                    base_pipeline.append(
                        {"$lookup": {
                            "from": related_collection,
                            "let": {f"foreign_{foreign_field}": f"${local_field}"
                                    for local_field, foreign_field in zip(primary_key, foreign_keys)},
                            "pipeline": [
                                {"$match": {"$expr": {"$and": match_conditions}}}
                            ],
                            "as": "joined_data"
                        }}
                    )
                    base_pipeline.append({"$unwind": {"path": "$joined_data", "preserveNullAndEmptyArrays": True}})
                elif isinstance(primary_key, str) and isinstance(foreign_keys, str):
                    # Single key case
                    base_pipeline.append(
                        {"$lookup": {
                            "from": related_collection,
                            "localField": primary_key,
                            "foreignField": foreign_keys,
                            "as": "joined_data"
                        }}
                    )
                    base_pipeline.append({"$unwind": {"path": "$joined_data", "preserveNullAndEmptyArrays": True}})
                else:
                    queries.append((collection_name, f"-- Mismatch in key lists or unsupported format for {related_collection}"))
        else:
            queries.append((collection_name, "-- No joinable relationship found for this collection."))

    # Handle GROUP BY if specified
    if include_group_by:
        if quantitative and categorical:
            numeric_field = random.choice(quantitative)
            category_field = random.choice(categorical)
            base_pipeline.append({"$group": {"_id": f"${category_field}", "average": {"$avg": f"${numeric_field}"}}})
        else:
            queries.append((collection_name, "-- Cannot generate 'GROUP BY' query due to insufficient data fields."))

    # Handle LIKE if specified
    if include_like:
        if categorical:
            category_field = random.choice(categorical)
            random_value = get_random_value_from_category(db, collection_name, category_field)
            if random_value:
                # Ensure `random_value` is a string before using it in `$regex`
                base_pipeline.insert(0, {"$match": {category_field: {"$regex": str(random_value), "$options": "i"}}})
            else:
                queries.append((collection_name, f"-- Could not generate LIKE query due to lack of data in {category_field}"))
        else:
            queries.append((collection_name, "-- Cannot generate 'LIKE' query due to lack of categorical fields."))

    # Handle ORDER BY if specified
    if include_order_by:
        if quantitative:
            numeric_field = random.choice(quantitative)
            order = random.choice([1, -1])  # 1 for ASC, -1 for DESC in MongoDB
            base_pipeline.append({"$sort": {numeric_field: order}})
        else:
            queries.append((collection_name, "-- Cannot generate 'ORDER BY' query due to lack of quantitative fields."))

    # Add the combined pipeline query if any components are generated
    if base_pipeline:
        queries.append((collection_name, base_pipeline))

    # Fallback if no keywords were matched
    if not queries:
        queries.append((collection_name, "-- No recognized constructs found in user input. Please try again with keywords like 'GROUP BY', 'LIKE', 'ORDER BY', or 'JOIN'."))

    return queries




# Modify the menu to include the new option
def menu():
    print("\n=== MongoDB Explorer Menu ===")
    print("1. List all collections in the database")
    print("2. View attributes (fields) of a collection")
    print("3. View sample data from a collection")
    print("4. Generate 5 random queries for a collection")
    print("5. Generate and execute a query based on your input construct")
    print("6. Generate and execute a query based on your input in natural language")
    print("q. Quit")
    return input("Select an option: ")

def main():
    db = connect_to_db()
    add_dataset_option()

    while True:
        choice = menu()

        if choice == '1':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

        elif choice == '2':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

            collection_choice = input("Select a collection by number to view its attributes: ")
            try:
                collection_index = int(collection_choice) - 1
                if 0 <= collection_index < len(collections):
                    selected_collection = collections[collection_index]
                    schema = get_collection_schema(db, selected_collection)
                    if schema:
                        print(f"\nAttributes of {selected_collection}:")
                        for field, field_type in schema.items():
                            print(f"Field: {field}, Type: {field_type}")
                        # Display the primary and foreign keys
                        primary_keys = analyze_collections()

                        for collection, details in primary_keys.items():
                            if(collection==selected_collection):
                                print(f"Collection: {collection}")
                                print(f"  Primary Key: {details['primary_key']}")
                                print(f"  Foreign Keys: {details['related_collections']}")
                    else:
                        print(f"The collection '{selected_collection}' is empty.")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '3':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

            collection_choice = input("Select a collection by number to view its sample data: ")
            try:
                collection_index = int(collection_choice) - 1
                if 0 <= collection_index < len(collections):
                    selected_collection = collections[collection_index]
                    sample_data = get_sample_data(db, selected_collection)
                    print(f"\nSample data from {selected_collection}:")
                    print(sample_data.head())
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '4':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

            collection_choice = input("Select a collection by number to generate queries: ")
            try:
                collection_index = int(collection_choice) - 1
                if 0 <= collection_index < len(collections):
                    selected_collection = collections[collection_index]
                    schema = get_collection_schema(db, selected_collection)
                    if schema:
                        quantitative, categorical = classify_fields(schema)
                        random_queries = generate_random_queries(selected_collection, quantitative, categorical, db,primary_keys)
                        print("\nGenerated Queries:")
                        for idx, query in enumerate(random_queries, start=1):
                            print(f"{idx}. {query}")
                    else:
                        print(f"The collection '{selected_collection}' is empty.")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '5':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

            collection_choice = input("Select a collection by number to generate a custom query: ")
            try:
                collection_index = int(collection_choice) - 1
                if 0 <= collection_index < len(collections):
                    selected_collection = collections[collection_index]
                    schema = get_collection_schema(db, selected_collection)
                    if schema:
                        quantitative, categorical = classify_fields(schema)
                        user_input = input("Enter your query construct (e.g., 'example query with group by'): ")
                        custom_queries = generate_custom_query(
                            selected_collection, quantitative, categorical, user_input, db
                        )
                        
                        print("\nGenerated Queries based on input:")
                        for idx, (collection, query) in enumerate(custom_queries, start=1):
                            if isinstance(query, str):  # For errors or comments
                                print(f"{query}")
                            else:
                                # Construct full query with db.collection_name
                                if isinstance(query, list):  # For aggregate queries
                                    full_query = f"db.{collection}.aggregate({query})"
                                elif isinstance(query, dict):  # For find queries
                                    full_query = f"db.{collection}.find({query})"
                                else:
                                    full_query = f"-- Unsupported query format: {query}"
                                
                                # Display the query
                                print(f"{full_query}")

                                # Ask user if they want to execute the query
                                execute_choice = input("\nDo you want to execute this query? (y/n): ")
                                if execute_choice.lower() == 'y':
                                    execute_query(db, collection, query)  # Execute and display the result of the query
                                else:
                                    print("Skipping execution of this query.")
                    else:
                        print(f"The collection '{selected_collection}' is empty.")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '6':
            collections = list_collections(db)
            print("\nAvailable collections:")
            for idx, collection in enumerate(collections, start=1):
                print(f"{idx}. {collection}")

            collection_choice = input("Select a collection by number for natural language query generation: ")
            try:
                collection_index = int(collection_choice) - 1
                if 0 <= collection_index < len(collections):
                    selected_collection = collections[collection_index]
                    schema = get_collection_schema(db, selected_collection)
                    if schema:
                        print("\nEnter a natural language query. Example:")
                        print("- 'Highest male employee by district code and year'")
                        print("- 'Find regions where sales are greater than 500'")
                        print("- 'Average sales grouped by region'")
                        user_input = input("Enter your natural language query: ")

                        nl_query = parse_natural_language_input(user_input, schema)
                        if isinstance(nl_query, str):
                            print(f"\nGenerated Query: {nl_query}")
                        else:
                            if isinstance(nl_query, list):
                                query_output = f"db.{selected_collection}.aggregate({nl_query})"
                            else:
                                query_output = f"db.{selected_collection}.find({nl_query})"

                            print(f"\nGenerated Query: {query_output}")

                            # Ask user if they want to execute the query
                            execute_choice = input("\nDo you want to execute this query? (y/n): ")
                            if execute_choice.lower() == 'y':
                                execute_query(db, selected_collection, nl_query)  # Execute and display the result of the query
                            else:
                                print("Skipping execution of this query.")
                    else:
                        print(f"The collection '{selected_collection}' is empty.")
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")


        elif choice.lower() == 'q':
            print("Exiting...")
            break

        else:
            print("Invalid option. Please select a valid menu option.")

if __name__ == "__main__":
    main()

