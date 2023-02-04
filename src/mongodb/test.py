import pymongo

from config import host


if __name__ == "__main__":
    # Create a database called "my_database":
    my_client = pymongo.MongoClient(host)
    my_db = my_client["my_database"]

    # Return a list of your system's databases:
    print(my_client.list_database_names())

    # Check if "my_database" exists
    db_list = my_client.list_database_names()
    if "my_database" in db_list:
        print("The database exists.")

