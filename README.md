# Work Search Data Pipeline

This project collects job postings for **Data Engineer** and **Machine Learning Engineer** roles from several public job sources in Northern Europe.

The goal is to build a simple data pipeline that can collect, clean, store, and analyze job market data.

## What the project does

The pipeline collects job ads from:

- JobTech Dev — Sweden
- NAV — Norway
- Adzuna — Germany and the Netherlands
- Arbeitnow — Germany, Netherlands, and remote jobs

It stores all results in one local SQLite database called:

```text
jobs.db
