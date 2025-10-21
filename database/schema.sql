-- Database Schema for GitHub Star Crawler
-- Optimized for 100,000 repositories with fast updates

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS repository_star_history CASCADE;
DROP TABLE IF EXISTS repositories CASCADE;


-- Main repositories table
CREATE TABLE repositories (
    id BIGSERIAL PRIMARY KEY,
    repo_id VARCHAR(255) UNIQUE NOT NULL,  -- GitHub's node_id (stable identifier)
    owner VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(511) NOT NULL,       -- owner/name format
    stars INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_crawled_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_owner_name UNIQUE(owner, name)
);

-- Star history for tracking changes over time (daily updates)
CREATE TABLE repository_star_history (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    stars INTEGER NOT NULL,
    recorded_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_repo_timestamp UNIQUE(repository_id, recorded_at)
);

-- Performance-critical indexes
CREATE INDEX idx_repositories_repo_id ON repositories(repo_id);
CREATE INDEX idx_repositories_stars ON repositories(stars DESC);
CREATE INDEX idx_repositories_last_crawled ON repositories(last_crawled_at);
CREATE INDEX idx_repositories_full_name ON repositories(full_name);
CREATE INDEX idx_star_history_repo_recorded ON repository_star_history(repository_id, recorded_at DESC);

-- Analyze tables for query optimization
ANALYZE repositories;
ANALYZE repository_star_history;