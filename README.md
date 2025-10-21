# GitHub Repository Crawler

A high-performance Python crawler that fetches repository data from GitHub's GraphQL API and stores it in PostgreSQL.

## 🎯 Features

- **Fast & Efficient**: Crawls 100,000 repositories in 1-2 minutes
- **Async Operations**: Uses asyncio and aiohttp for parallel requests
- **Rate Limit Handling**: Smart rate limiting with automatic retry
- **Database Optimized**: Batch inserts with connection pooling
- **Automated**: GitHub Actions for daily scheduled runs
- **GraphQL API**: Uses GitHub's GraphQL API for efficient data fetching

## 🚀 Quick Start

### Prerequisites

- Python 3.13+
- PostgreSQL database
- GitHub Personal Access Token

### Installation

1. Clone the repository:

```bash
git clone https://github.com/Arfa-Ahsan/Github-Crawler.git
cd Github-Crawler
```

2. Create a virtual environment and install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables (copy `.env.example` to `.env`):

```env
GITHUB_TOKEN=your_github_token_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=github_crawler
DB_USER=postgres
DB_PASSWORD=your_password
```

4. Initialize the database:

```bash
psql -h localhost -U postgres -d github_crawler -f database/schema.sql
```

5. Run the crawler:

```bash
python main.py
```

## ⚙️ Automated Daily Crawling with GitHub Actions

This project includes a **zero-configuration** GitHub Actions workflow!

### 🎯 Features

✅ **No secrets required** - Uses default `GITHUB_TOKEN`  
✅ **PostgreSQL service container** - Built-in database  
✅ **Automatic schema setup** - Creates tables on each run  
✅ **CSV exports** - Download results as artifacts  
✅ **Daily schedule** - Runs automatically at 2 AM UTC

### 🚀 Quick Setup

**No configuration needed!** Just push to GitHub:

```bash
git add .
git commit -m "Add GitHub Actions crawler"
git push origin main
```

The workflow automatically:

1. Spins up PostgreSQL in a container
2. Creates database schema
3. Crawls 100,000 repositories
4. Exports data to CSV
5. Uploads as artifacts (downloadable for 30 days)

### 🤔 "Wait, how does the token work?"

Great question! Your `.env` file stays on your computer (it's in `.gitignore`).

GitHub Actions uses a **different token** that GitHub provides automatically!

**📖 Read more:**

- **[Quick Answer](QUICK_TOKEN_ANSWER.md)** - 2-minute explanation
- **[Visual Guide](TOKEN_VISUAL_GUIDE.md)** - Diagrams and flowcharts
- **[Full Explanation](GITHUB_TOKEN_EXPLANATION.md)** - Complete technical details

**TL;DR:** Local development uses your `.env` file. GitHub Actions uses an automatic token. Both work with the same code! 4. Exports data as CSV artifacts 5. Stores results for 30 days

### 📊 View Results

After a run completes:

1. Go to **Actions** tab on GitHub
2. Click on the completed workflow
3. Download **Artifacts** (CSV files)
4. Analyze the data!

**📖 [Full Guide: GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)**

## 📊 Performance

- **Target**: 100,000 repositories
- **Time**: 1-2 minutes (optimized version)
- **Rate**: 200-300+ repos/second
- **API Calls**: ~5,000/hour (within GitHub limits)

### Multi-Query Strategy

The crawler uses 400+ different search queries to bypass GitHub's 1,000 result limit per query:

- 10 programming languages
- 8 years (2018-2025)
- 5 star buckets (1-10, 10-50, 50-200, 200-1k, 1k-10k)
- Total capacity: 400,000+ repositories

## 🗄️ Database Schema

### Tables

**repositories**

- `id`: Primary key
- `repo_id`: GitHub repository ID (unique)
- `owner`: Repository owner
- `name`: Repository name
- `full_name`: Full name (owner/repo)
- `stars`: Current star count
- `created_at`: Repository creation date
- `updated_at`: Last update date
- `last_crawled_at`: Last crawl timestamp

**repository_star_history**

- `id`: Primary key
- `repository_id`: Foreign key to repositories
- `stars`: Star count at recording time
- `recorded_at`: Timestamp (default: current date)

## 📖 Documentation

- **[Pipeline Implementation](PIPELINE_IMPLEMENTATION.md)** - Complete requirements checklist
- **[GitHub Actions Setup](GITHUB_ACTIONS_SETUP.md)** - Detailed setup guide
- **[Workflow Documentation](.github/workflows/README.md)** - Technical workflow details
- **[Optimization Guide](OPTIMIZATION_GUIDE.md)** - Performance optimizations explained
- **[Quick Start Guide](QUICKSTART.md)** - 5-minute local setup

## 🎯 GitHub Actions Requirements

This project implements a complete GitHub Actions pipeline with:

1. ✅ **PostgreSQL service container** - Fresh database per run
2. ✅ **Setup & dependency installs** - Python 3.13 + requirements
3. ✅ **Setup-postgres step** - Creates tables from schema.sql
4. ✅ **Crawl-stars step** - Fetches 100,000 repositories via GraphQL
5. ✅ **Database dump & artifacts** - Exports CSV files (30-day retention)
6. ✅ **Default GitHub token** - No secrets required, no elevated permissions

**📋 [See complete implementation details](PIPELINE_IMPLEMENTATION.md)**

## 🛠️ Configuration

Edit configuration in `main.py`:

```python
BATCH_SIZE = 100              # Repos per GraphQL query
DB_BATCH_SIZE = 1000          # Buffer size before DB insert
MAX_CONCURRENT_REQUESTS = 15  # Parallel requests
DB_POOL_MIN = 3               # Min DB connections
DB_POOL_MAX = 10              # Max DB connections
```

## 🔒 Security

- Never commit `.env` file
- Use environment variables for secrets
- Rotate GitHub tokens regularly
- Use read-only database user in production

## Notes

- The project expects a Postgres database reachable using the credentials in the `.env` file.
- Keep `.env` out of version control — use `.env.example` for shareable configuration.

## 📝 License

MIT License

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
