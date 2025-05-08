# modules/validator.py
import psycopg2
import pymysql
import pyodbc
import sqlite3
import pymongo


def validate_db_connection(db_type, creds):
    """
    Validates connection to the source or target database based on the provided credentials.
    """
    try:
        if db_type == "PostgreSQL":
            connection = psycopg2.connect(
                host=creds['host'],
                port=creds['port'],
                user=creds['user'],
                password=creds['password'],
                database=creds['database']
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return True, result

        elif db_type == "MySQL":
            connection = pymysql.connect(
                host=creds['host'],
                port=int(creds['port']),
                user=creds['user'],
                password=creds['password'],
                database=creds['database']
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return True, result

        elif db_type == "MSSQL":
            connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={creds["host"]};PORT={creds["port"]};UID={creds["username"]};PWD={creds["password"]};DATABASE={creds["database"]}'
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT TOP 2 * FROM {creds['table']};")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return True, result

        elif db_type == "MongoDB":
            client = pymongo.MongoClient(creds['uri'])
            db = client[creds['database']]
            collection = db[creds['collection']]
            result = list(collection.find().limit(2))
            client.close()
            return True, result

        elif db_type == "SQLite":
            connection = sqlite3.connect(creds['file_path'])
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return True, result

        else:
            return False, "Unsupported database type"
    
    except Exception as e:
        return False, str(e)


# def validate_and_fetch_schema(db_type, creds):
#     """
#     Validates the database connection and fetches the schema (table/collection preview).
#     Returns a status message, preview data, and schema (column names or field structure).
#     """
#     try:
#         if db_type == "PostgreSQL":
#             connection = psycopg2.connect(
#                 host=creds["host"],
#                 port=creds["port"],
#                 user=creds["user"],
#                 password=creds["password"],
#                 dbname=creds["database"]
#             )
#             cursor = connection.cursor()
#             cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
#             preview = cursor.fetchall()
#             schema = [desc[0] for desc in cursor.description]
#             status = "Connected to PostgreSQL successfully!"

#         elif db_type == "MySQL":
#             connection = pymysql.connect(
#                 host=creds["host"],
#                 port=int(creds["port"]),
#                 user=creds["user"],
#                 password=creds["password"],
#                 database=creds["database"]
#             )
#             cursor = connection.cursor()
#             cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
#             preview = cursor.fetchall()
#             print("===============neww============")
#             print(preview)
#             schema = [desc[0] for desc in cursor.description]
#             print(schema)
#             status = "Connected to MySQL successfully!"

#         elif db_type == "MSSQL":
#             connection = pyodbc.connect(
#                 f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={creds["host"]};PORT={creds["port"]};DATABASE={creds["database"]};UID={creds["username"]};PWD={creds["password"]}'
#             )
#             cursor = connection.cursor()
#             cursor.execute(f"SELECT TOP 2 * FROM {creds['table']}")
#             preview = cursor.fetchall()
#             schema = [column[0] for column in cursor.description]
#             status = "Connected to MSSQL successfully!"

#         elif db_type == "SQLite":
#             connection = sqlite3.connect(creds["file_path"])
#             cursor = connection.cursor()
#             cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
#             preview = cursor.fetchall()
#             schema = [desc[0] for desc in cursor.description]
#             status = "Connected to SQLite successfully!"

#         elif db_type == "MongoDB":
#             client = pymongo.MongoClient(creds["uri"])
#             db = client[creds["database"]]
#             collection = db[creds["collection"]]
#             preview = list(collection.find().limit(2))
#             if preview:
#                 schema = list(preview[0].keys())
#             else:
#                 schema = []
#             status = "Connected to MongoDB successfully!"

#         return status, preview, schema

#     except Exception as e:
#         return f"Error: {str(e)}", [], []
def validate_and_fetch_schema(db_type, creds):
    try:
        if db_type == "PostgreSQL":
            connection = psycopg2.connect(
                host=creds["host"],
                port=creds["port"],
                user=creds["user"],
                password=creds["password"],
                dbname=creds["database"]
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            preview = cursor.fetchall()
            schema = [
                {"name": desc.name, "type": desc.type_code} for desc in cursor.description
            ]
            status = "Connected to PostgreSQL successfully!"

        elif db_type == "MySQL":
            connection = pymysql.connect(
                host=creds["host"],
                port=int(creds["port"]),
                user=creds["user"],
                password=creds["password"],
                database=creds["database"]
            )
            cursor = connection.cursor()
            cursor.execute(f"DESCRIBE {creds['table']};")
            desc_rows = cursor.fetchall()
            schema = [
                {"name": row[0], "type": row[1], "nullable": row[2] != "NO"} for row in desc_rows
            ]
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            preview = cursor.fetchall()
            status = "Connected to MySQL successfully!"

        elif db_type == "MSSQL":
            connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={creds["host"]};PORT={creds["port"]};DATABASE={creds["database"]};UID={creds["username"]};PWD={creds["password"]}'
            )
            cursor = connection.cursor()
            query = f"SELECT TOP 2 * FROM {creds['table']}"
            cursor.execute(query)
            preview = cursor.fetchall()
            schema = [
                {"name": column[0], "type": str(column[1])} for column in cursor.description
            ]
            status = "Connected to MSSQL successfully!"

        elif db_type == "SQLite":
            connection = sqlite3.connect(creds["file_path"])
            cursor = connection.cursor()
            cursor.execute(f"PRAGMA table_info({creds['table']});")
            info = cursor.fetchall()
            schema = [
                {"name": row[1], "type": row[2], "nullable": row[3] == 0} for row in info
            ]
            cursor.execute(f"SELECT * FROM {creds['table']} LIMIT 2;")
            preview = cursor.fetchall()
            status = "Connected to SQLite successfully!"

        elif db_type == "MongoDB":
            client = pymongo.MongoClient(creds["uri"])
            db = client[creds["database"]]
            collection = db[creds["collection"]]
            preview = list(collection.find().limit(2))
            if preview:
                schema = [{"name": key, "type": type(value).__name__} 
                          for key, value in preview[0].items()]
            else:
                schema = []
            status = "Connected to MongoDB successfully!"

        return status, preview, schema

    except Exception as e:
        return f"Error: {str(e)}", [], []
