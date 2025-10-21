# Github Crawler

A small utility for crawling GitHub data and storing it in a Postgres database.

## Overview

This project fetches data from GitHub (optionally using a personal access token), applies retry and rate-limiting logic, validates the data, and stores results in a Postgres database.

## Getting started

1. Create a virtual environment and install dependencies:

   pip install -r requirements.txt

2. Copy `.env.example` to `.env` and fill in your configuration values (database credentials, GitHub token if needed).

3. Run the main script:

   python main.py

## Notes

- The project expects a Postgres database reachable using the credentials in the `.env` file.
- Keep `.env` out of version control — use `.env.example` for shareable configuration.
