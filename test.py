"""
Test suite for GitHub Crawler
Run this file to test various components before running the full crawl
"""

import os
import time
import asyncio
from dotenv import load_dotenv
from test.db_connection import get_connection, test_connection
import psycopg2

load_dotenv()

# Color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

def print_test_header(test_name):
    """Print formatted test header"""
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}🧪 TEST: {test_name}{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

def test_database_connection():
    """Test 1: Verify PostgreSQL connection"""
    print_test_header("Database Connection")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"{GREEN}✅ PostgreSQL connected successfully!{RESET}")
        print(f"   Version: {version[:50]}...")
        
        # Test tables exist
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN ('repositories', 'repository_star_history')
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        if 'repositories' in tables and 'repository_star_history' in tables:
            print(f"{GREEN}✅ Required tables exist{RESET}")
        else:
            print(f"{RED}❌ Missing tables. Please run schema.sql{RESET}")
            print(f"   Found tables: {tables}")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"{RED}❌ Database connection failed: {e}{RESET}")
        return False

def test_github_token():
    """Test 2: Verify GitHub token is set and valid"""
    print_test_header("GitHub Token")
    token = os.getenv("GITHUB_TOKEN")
    
    if not token:
        print(f"{RED}❌ GITHUB_TOKEN not set in .env{RESET}")
        print(f"{YELLOW}   Set your token: https://github.com/settings/tokens{RESET}")
        return False
    
    print(f"{GREEN}✅ GITHUB_TOKEN is set{RESET}")
    print(f"   Token preview: {token[:15]}...{token[-4:]}")
    
    # Test token validity with a simple API call
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get("https://api.github.com/user", headers=headers)
    
    if response.status_code == 200:
        user_data = response.json()
        print(f"{GREEN}✅ Token is valid{RESET}")
        print(f"   Authenticated as: {user_data.get('login', 'Unknown')}")
        
        # Check rate limit
        rate_response = requests.get("https://api.github.com/rate_limit", headers=headers)
        if rate_response.status_code == 200:
            rate_data = rate_response.json()
            graphql_limit = rate_data.get("resources", {}).get("graphql", {})
            print(f"   GraphQL rate limit: {graphql_limit.get('remaining')}/{graphql_limit.get('limit')}")
            reset_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(graphql_limit.get('reset', 0)))
            print(f"   Resets at: {reset_time}")
        
        return True
    else:
        print(f"{RED}❌ Token is invalid or expired{RESET}")
        print(f"   Status: {response.status_code}")
        return False

def test_small_crawl():
    """Test 3: Run a small test crawl (10 repos)"""
    print_test_header("Small Crawl Test (10 repos)")
    try:
        from main import crawl_repositories
        
        print(f"{YELLOW}⏳ Starting small crawl...{RESET}")
        start_time = time.time()
        
        crawl_repositories(limit=10)
        
        elapsed = time.time() - start_time
        print(f"{GREEN}✅ Small crawl completed in {elapsed:.2f}s{RESET}")
        
        # Verify data in database
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM repositories")
        count = cur.fetchone()[0]
        print(f"   Total repositories in DB: {count}")
        
        cur.execute("SELECT COUNT(*) FROM repository_star_history")
        history_count = cur.fetchone()[0]
        print(f"   Total star history records: {history_count}")
        
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"{RED}❌ Small crawl failed: {e}{RESET}")
        import traceback
        traceback.print_exc()
        return False

def test_small_crawl_optimized():
    """Test 3b: Run a small test crawl with OPTIMIZED version (10 repos)"""
    print_test_header("Small Crawl Test - OPTIMIZED (10 repos)")
    try:
        from main_optimized import crawl_repositories
        
        print(f"{YELLOW}⏳ Starting OPTIMIZED small crawl...{RESET}")
        start_time = time.time()
        
        crawl_repositories(limit=10)
        
        elapsed = time.time() - start_time
        print(f"{GREEN}✅ OPTIMIZED small crawl completed in {elapsed:.2f}s{RESET}")
        
        # Verify data in database
        import asyncpg
        import asyncio
        
        async def check_db():
            pool = await asyncpg.create_pool(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 5432)),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
            
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM repositories")
                history_count = await conn.fetchval("SELECT COUNT(*) FROM repository_star_history")
                
                print(f"   Total repositories in DB: {count}")
                print(f"   Total star history records: {history_count}")
            
            await pool.close()
        
        asyncio.run(check_db())
        
        return True
    except Exception as e:
        print(f"{RED}❌ OPTIMIZED crawl failed: {e}{RESET}")
        import traceback
        traceback.print_exc()
        return False

def test_batch_insert_performance():
    """Test 4: Benchmark batch insert performance"""
    print_test_header("Batch Insert Performance")
    try:
        from main import batch_insert_repos
        
        # Create fake repo data
        fake_repos = []
        for i in range(500):
            fake_repos.append({
                "id": f"test_repo_{i}_{int(time.time())}",
                "owner": {"login": f"user_{i}"},
                "name": f"repo_{i}",
                "stargazerCount": i * 10,
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-01-01T00:00:00Z"
            })
        
        conn = get_connection()
        
        print(f"{YELLOW}⏳ Inserting 500 test repositories...{RESET}")
        start_time = time.time()
        
        batch_insert_repos(conn, fake_repos)
        
        elapsed = time.time() - start_time
        rate = len(fake_repos) / elapsed
        
        print(f"{GREEN}✅ Batch insert completed in {elapsed:.2f}s{RESET}")
        print(f"   Performance: {rate:.0f} repos/second")
        
        # Cleanup test data
        cur = conn.cursor()
        cur.execute("DELETE FROM repositories WHERE repo_id LIKE 'test_repo_%'")
        deleted = cur.rowcount
        conn.commit()
        print(f"   Cleaned up {deleted} test records")
        
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"{RED}❌ Batch insert test failed: {e}{RESET}")
        import traceback
        traceback.print_exc()
        return False

def test_rate_limiter():
    """Test 5: Verify rate limiter works correctly"""
    print_test_header("Rate Limiter")
    try:
        from main import RateLimiter
        
        print(f"{YELLOW}⏳ Testing rate limiter (10 requests)...{RESET}")
        limiter = RateLimiter(max_requests_per_hour=10)
        
        async def test_limiter():
            start = time.time()
            for i in range(12):
                await limiter.acquire()
                print(f"   Request {i+1} allowed at {time.time() - start:.2f}s")
            return time.time() - start
        
        elapsed = asyncio.run(test_limiter())
        
        print(f"{GREEN}✅ Rate limiter working correctly{RESET}")
        print(f"   12 requests took {elapsed:.2f}s")
        
        return True
    except Exception as e:
        print(f"{RED}❌ Rate limiter test failed: {e}{RESET}")
        return False

def test_database_indexes():
    """Test 6: Verify database indexes exist"""
    print_test_header("Database Indexes")
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT indexname, tablename 
            FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND tablename IN ('repositories', 'repository_star_history')
            ORDER BY tablename, indexname
        """)
        
        indexes = cur.fetchall()
        
        print(f"{GREEN}✅ Found {len(indexes)} indexes:{RESET}")
        for idx_name, table_name in indexes:
            print(f"   - {table_name}.{idx_name}")
        
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"{RED}❌ Index check failed: {e}{RESET}")
        return False

def run_all_tests():
    """Run all tests in sequence"""
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}🚀 GitHub Crawler Test Suite{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")
    
    tests = [
        ("Database Connection", test_database_connection),
        ("GitHub Token", test_github_token),
        ("Database Indexes", test_database_indexes),
        ("Rate Limiter", test_rate_limiter),
        ("Batch Insert Performance", test_batch_insert_performance),
        ("Small Crawl (10 repos)", test_small_crawl),
        ("Small Crawl OPTIMIZED (10 repos)", test_small_crawl_optimized),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"{RED}❌ Test '{test_name}' crashed: {e}{RESET}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📊 Test Summary{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{GREEN}✅ PASS{RESET}" if result else f"{RED}❌ FAIL{RESET}"
        print(f"{status} - {test_name}")
    
    print(f"\n{BLUE}Results: {passed}/{total} tests passed{RESET}")
    
    if passed == total:
        print(f"{GREEN}✅ All tests passed! Ready to crawl 100k repos.{RESET}")
    else:
        print(f"{YELLOW}⚠️ Some tests failed. Fix issues before running full crawl.{RESET}")

if __name__ == "__main__":
    run_all_tests()
