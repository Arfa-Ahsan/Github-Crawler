# db_client.py
import sys
import os
from pathlib import Path

# Add parent directory to path so we can import from test module
sys.path.insert(0, str(Path(__file__).parent.parent))

from test.db_connection import get_connection

def init_db():
    """Initialize the database schema."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                schema_path = Path(__file__).parent / "schema.sql"
                with open(schema_path, "r") as f:
                    schema_sql = f.read()
                cur.execute(schema_sql)
            conn.commit()
        print("✅ Database schema created successfully.")
    except Exception as e:
        print("❌ Failed to initialize database schema:", e)


if __name__ == "__main__":
    print("🚀 Initializing database schema...")
    init_db()

