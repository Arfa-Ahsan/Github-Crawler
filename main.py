import os
import time
import requests
import psycopg2
from dotenv import load_dotenv
from test.db_connection import get_connection

load_dotenv()

GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # For local testing only

# GraphQL query to fetch repositories
QUERY = """
{
  search(query: "stars:>1", type: REPOSITORY, first: 100, after: AFTER_CURSOR) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        id
        name
        owner { login }
        stargazerCount
        createdAt
        updatedAt
      }
    }
  }
}
"""

def run_query(after_cursor=None):
    query = QUERY.replace("AFTER_CURSOR", f'"{after_cursor}"' if after_cursor else "null")
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    response = requests.post(GITHUB_API_URL, json={"query": query}, headers=headers)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 502:
        print("⚠️ Bad gateway, retrying in 10s...")
        time.sleep(10)
        return run_query(after_cursor)
    else:
        raise Exception(f"Query failed: {response.status_code} - {response.text}")

def insert_or_update_repo(cur, repo):
    cur.execute("""
        INSERT INTO repositories (repo_id, owner, name, full_name, stars, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (repo_id)
        DO UPDATE SET stars = EXCLUDED.stars, updated_at = EXCLUDED.updated_at, last_crawled_at = NOW()
        RETURNING id;
    """, (
        repo["id"],
        repo["owner"]["login"],
        repo["name"],
        f'{repo["owner"]["login"]}/{repo["name"]}',
        repo["stargazerCount"],
        repo["createdAt"],
        repo["updatedAt"]
    ))
    repo_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO repository_star_history (repository_id, stars)
        VALUES (%s, %s)
        ON CONFLICT (repository_id, recorded_at) DO NOTHING;
    """, (repo_id, repo["stargazerCount"]))

def crawl_repositories(limit=100000):
    conn = get_connection()
    cur = conn.cursor()
    after_cursor = None
    total_fetched = 0

    while total_fetched < limit:
        data = run_query(after_cursor)
        repos = data["data"]["search"]["nodes"]
        page_info = data["data"]["search"]["pageInfo"]

        for repo in repos:
            insert_or_update_repo(cur, repo)
            total_fetched += 1
            if total_fetched >= limit:
                break

        conn.commit()
        print(f"✅ {total_fetched} repositories processed...")

        if not page_info["hasNextPage"]:
            break
        after_cursor = page_info["endCursor"]

        # Respect GitHub rate limits
        time.sleep(2)

    cur.close()
    conn.close()
    print("✅ Crawl completed successfully.")

if __name__ == "__main__":
    crawl_repositories()
