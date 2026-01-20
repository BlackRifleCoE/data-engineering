# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DimensionProcessor is a Python library for managing Slowly Changing Dimensions (SCD) in data warehouses using PySpark and Delta Lake. It provides two main classes for handling Type 1 and Type 2 dimension updates.

## Environment Setup

The project uses a Python virtual environment located in `.venv/`. Activate it before working:

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

## Architecture

### Type 1 Dimension (SCD.py)

The `Type1Dimension` class implements overwrite semantics for dimension updates:
- Performs outer join between source and target DataFrames
- Updates columns in-place without historical tracking
- Located in [SCD.py](SCD.py)

**Key Method**: `ProcessUpdate()` - returns updated target DataFrame with merged changes

### Type 2 Dimension (SCD2.py)

The `Type2Dimension` class implements historical tracking with effective dates:
- Uses hash-based change detection (SHA-256 on specified columns)
- Maintains version history with effective start/end dates
- Manages surrogate keys and current flag (`Is_Current`)
- Integrates with Delta Lake for merge operations
- Located in [SCD2.py](SCD2.py)

**Key Method**: `ProcessUpdate()` - returns count of inserted records

#### SCD2 Processing Flow

1. **Hash Generation**: Creates row_hash from specified columns for change detection
2. **Change Identification**: Left joins bronze data with current dimension records
3. **Historical Record Management**:
   - Expires changed records by setting `Effective_End_Date` and `Is_Current = 0`
   - Uses Delta Lake merge operation for updates
4. **New Record Insertion**:
   - Auto-increments surrogate key
   - Sets `Effective_Start_Date` from LoadTimestamp
   - Marks as current (`Is_Current = 1`)

### Key Class Parameters

**Type2Dimension constructor**:
- `bronzeDF`: Source DataFrame with new data (must include `LoadTimestamp`)
- `dimensionDF`: Existing dimension DataFrame (or empty string for initialization)
- `dimensionTableName`: Delta table name for merge operations
- `hashColumnList`: Columns used for change detection hash
- `primaryKeyColunmName`: Business key column (note: typo in parameter name)
- `surrogateKeyColumnName`: Auto-incrementing technical key

### Delta Lake Integration

The Type 2 processor uses Delta Lake's merge functionality for atomic updates. Ensure:
- Target tables are Delta format
- SparkSession has Delta extensions enabled
- Table names are valid in the current catalog/schema

### Required DataFrame Schemas

**Bronze DataFrame** (input to SCD2):
- Must contain all columns in `hashColumnList`
- Must include `LoadTimestamp` column

**Dimension DataFrame** (existing SCD2 table):
- Includes all business columns
- Plus metadata: `{surrogateKeyColumnName}`, `row_hash`, `Effective_Start_Date`, `Effective_End_Date`, `Is_Current`

## Dependencies

- pyspark (including pyspark.sql and pyspark.sql.functions)
- delta-spark (Delta Lake integration)
- Python standard library: typing, logging
