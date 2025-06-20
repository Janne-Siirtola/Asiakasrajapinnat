import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential
import pyodbc

#NEWEST!!

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger: creating test table.')

    server   = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    driver   = "{ODBC Driver 18 for SQL Server}"

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};DATABASE={database};"
        "Authentication=ActiveDirectoryMsi;"
        "Encrypt=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        if not conn:
            logging.error("Failed to connect to the database.")
            return func.HttpResponse("Failed to connect to the database.", status_code=500)
        if not cursor:
            logging.error("Failed to create a cursor.")
            return func.HttpResponse("Failed to create a cursor.", status_code=500)

        # 3) Create a simple test table
        cursor.execute("""
            IF OBJECT_ID('dbo.TestFunctionTable', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.TestFunctionTable (
                    Id INT IDENTITY PRIMARY KEY,
                    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
                );
            END
        """)
        conn.commit()

    except Exception as e:
        logging.error(f"Error creating table: {e}")
        return func.HttpResponse(f"Failed to create table: {e}", status_code=500)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return func.HttpResponse("✅ Test table created (or already existed).", status_code=200)
