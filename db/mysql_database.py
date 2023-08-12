from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv,find_dotenv
import os

# Find .env file
load_dotenv(find_dotenv())

class Database:
    def __init__(self, user = os.getenv('DB_USER'), password = os.getenv('DB_PASSWORD'), host= os.getenv('DB_HOST'), database= os.getenv('DB_DATABASE')):
        self.user =  user
        self.password = password
        self.host = host
        self.database = database
        self.connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.engine = None
        self.Session = None

    def connect(self):
        try:
            self.engine = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.engine)
            return True
        except exc.SQLAlchemyError as e:
            print(f"Error connecting to database: {e}")
            return False

    def execute_select(self, query, params=None):
        """Execute a SELECT SQL query."""
        if not self.Session:
            raise Exception("Database connection is not established. Call connect() first.")
        session = self.Session()
        try:
            result = session.execute(text(query), params)
            return result.fetchall()
        except exc.SQLAlchemyError as e:
            print(f"Error executing SELECT query: {e}")
            return []
        finally:
            session.close()

    def execute_insert(self, table, values):
        """Execute an INSERT SQL query."""
        if not self.Session:
            raise Exception("Database connection is not established. Call connect() first.")
        session = self.Session()
        query = f"INSERT INTO {table} ({', '.join(values.keys())}) VALUES ({', '.join([':' + key for key in values.keys()])})"
        try:
            session.execute(text(query), values)
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            print(f"Error executing INSERT query: {e}")
        finally:
            session.close()

    def execute_select_with_column_name(self, query, params=None):
        """Execute a SELECT SQL query and return both rows and column names."""
        if not self.Session:
            raise Exception("Database connection is not established. Call connect() first.")
        
        session = self.Session()
        try:
            result = session.execute(text(query), params)
            # Get the column names using keys() method
            column_names = result.keys()
            rows = result.fetchall()
            # Return both column names and rows
            return rows, column_names
        except exc.SQLAlchemyError as e:
            print(f"Error executing SELECT query: {e}")
            return [], []
        finally:
            session.close()