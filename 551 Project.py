import os

def sql_implementation():
    print("Executing SQL implementation...")
    os.system(r'python "D:\USC- Academics\DSCI 551 Data management\Project\sql_implementation.py"')

def nosql_implementation():
    print("Executing NoSQL implementation...")
    os.system(r'python "D:\USC- Academics\DSCI 551 Data management\Project\NoSQL_implementation.py"')

def main():
    while True:
        print("\nChoose an option:")
        print("1. SQL Implementation")
        print("2. NoSQL Implementation")
        print("3. Quit")

        try:
            choice = int(input("Enter your choice (1/2/3): ").strip())
            
            if choice == 1:
                print("\n--- Starting SQL Implementation ---")
                sql_implementation()
                print("\nExited SQL Implementation.")
            
            elif choice == 2:
                print("\n--- Starting NoSQL Implementation ---")
                nosql_implementation()
                print("\nExited NoSQL Implementation.")
            
            elif choice == 3:
                print("\nExiting the program. Goodbye!")
                break

            else:
                print("Invalid choice. Please enter 1, 2, or 3.")

        except ValueError:
            print("Invalid input. Please enter a number (1/2/3).")

# Run the main menu
if __name__ == "__main__":
    main()
