import os
from dotenv import load_dotenv

# Assuming you've separated each class into its module.
from lark.lark_authenticator import LarkAuthenticator
from lark.lark_contact_manager import LarkContactManager
from lark.bitable_manager import BitableManager
from db.mysql_database import Database


load_dotenv()

def main():

    # 1. Authenticate with Lark
    lark_auth = LarkAuthenticator()

    # 2. Fetch Lark user IDs for a list of agent emails.
    lark_contact_manager = LarkContactManager(lark_auth)
    agent_emails = ["agent1@example.com", "agent2@example.com"]
    user_ids = lark_contact_manager.get_lark_user_ids(agent_emails)
    print("Lark User IDs:", user_ids)

    # 3. Retrieve records from a Bitable table.
    bitable_manager = BitableManager(lark_auth)
    table_id = "YOUR_BITABLE_TABLE_ID"
    records = bitable_manager.get_records(table_id)
    print("Bitable Records:", records)

    # 4. Add a new record to the Bitable table.
    new_record = {
        "ColumnName1": "Value1",
        "ColumnName2": "Value2",
    }
    response = bitable_manager.add_rows_to_bitable(table_id, new_record)
    print("Added Record Response:", response)

    db = Database()
    if db.connect():
        # SELECT Query example
        users = db.execute_select("SELECT * FROM users WHERE name = :name", {"name": "John Doe"})
        for user in users:
            print(user)

        # INSERT Query example
        db.execute_insert("users", {"name": "Jane Doe", "email": "jane@example.com"})


if __name__ == "__main__":
    main()
