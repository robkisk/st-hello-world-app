# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a Streamlit application that connects to Databricks via Databricks Connect to analyze NYC taxi data. The app consists of two main components:

- **app.py**: Main Streamlit app displaying Uber pickups data from `samples.nyctaxi.trips` table
- **app_survey.py**: Store closure survey form that saves responses to Delta tables

The application uses a dual-database approach:
- Primary: Databricks Delta tables (`bu1_dev.analytics.uber_pickups`, `bu1_dev.analytics.surveys`)  
- Fallback: Local CSV files when Databricks connection fails

## Key Dependencies

- **Databricks Connect**: Connects to Databricks serverless compute
- **Polars**: Primary DataFrame library (preferred over pandas)
- **Streamlit**: Web app framework
- **PySpark**: Spark DataFrame operations via Databricks Connect

## Common Commands

### Development
```bash
# Install dependencies
uv sync

# Run main Streamlit app (NYC taxi data)
streamlit run app.py

# Run survey app
streamlit run app_survey.py

# Run standalone Python script
uv run main.py

# Install new dependency
uv add <package-name>
```

### Testing and Linting
```bash
# Run tests
uv run pytest

# Run linter/formatter
uv run ruff check
uv run ruff format
```

### Data Utilities
The `utils.py` module provides:
- `get_spark_session()`: Creates Databricks serverless session
- `show_sample_data()`: Display sample data from Unity Catalog tables
- Workspace info utilities

## Development Guidelines

### Python Style
- Use type annotations on all functions and classes
- Add descriptive docstrings following PEP257
- Prefer functional programming over OOP
- Target Python 3.12+
- Always use descriptive variable names

### Data Processing
- Use Polars for DataFrame operations (preferred over pandas)
- Convert Polars → pandas → Spark when saving to Delta tables
- Always include fallback to local CSV when Delta table save fails

### Jupyter/Databricks Integration
- `# COMMAND ----------` comments mark cell boundaries (don't remove)
- Files with `# Databricks notebook source` header are Databricks notebooks
