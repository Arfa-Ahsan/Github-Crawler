# db_client.py
from test.db_connection import get_connection

def init_db():
    """Initialize the database schema."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                with open("schema.sql", "r") as f:
                    schema_sql = f.read()
                cur.execute(schema_sql)
            conn.commit()
        print("✅ Database schema created successfully.")
    except Exception as e:
        print("❌ Failed to initialize database schema:", e)
