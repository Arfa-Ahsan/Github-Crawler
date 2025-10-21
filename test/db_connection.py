# test/test_db_connection.py
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Load environment variables from .env
load_dotenv()

def get_connection():
    """Create a connection to PostgreSQL using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    except Exception as e:
        print("❌ Error connecting to PostgreSQL:", e)
        raise

def test_connection():
    """Test whether PostgreSQL connection works."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("✅ PostgreSQL connected successfully!")
        print("🧠 Version:", version[0])

        cur.close()
        conn.close()
    except Exception as e:
        print("❌ Connection test failed:", e)

if __name__ == "__main__":
    test_connection()