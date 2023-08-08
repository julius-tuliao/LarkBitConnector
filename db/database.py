from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

class Database:
    def __init__(self, user, password, host, database):
        self.user = user
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