# DimensionProcessor Test Suite

Comprehensive test suite for the DimensionProcessor library (SCD Type 1 and Type 2).

## Overview

This test suite provides **comprehensive coverage** of all SCD2 scenarios including:
- Hash generation and change detection
- Empty dimension bootstrapping
- Record updates and versioning
- Unchanged record handling
- Edge cases (nulls, unicode, empty datasets, etc.)
- Delta Lake integration
- Performance testing

**Total Tests:** 30+ tests across multiple categories

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                    # SparkSession fixtures and test data
├── test_type2_dimension.py        # Core SCD2 tests (hash, init, bootstrap)
├── test_type2_edge_cases.py       # Edge cases and error handling
├── test_type2_integration.py      # Delta Lake integration tests
├── test_type1_dimension.py        # Type1 dimension tests
├── helpers/
│   ├── __init__.py
│   ├── data_generators.py         # Test data creation utilities
│   └── assertions.py              # Custom assertion helpers
└── README.md                      # This file
```

## Installation

### Install Test Dependencies

```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dev dependencies
pip install -r requirements-dev.txt
```

### Dependencies

- pytest >= 7.4.0
- pytest-spark >= 0.6.0
- pyspark >= 3.4.0
- delta-spark >= 2.4.0
- chispa >= 0.9.0 (DataFrame comparison)
- coverage >= 7.0.0
- pytest-cov >= 4.1.0

## Running Tests

### Run All Tests

```bash
pytest
```

### Run with Verbose Output

```bash
pytest -v
```

### Run with Coverage Report

```bash
pytest --cov=. --cov-report=html
```

Coverage report will be generated in `htmlcov/index.html`.

### Run Specific Test Categories

```bash
# Unit tests only (fast)
pytest -m unit

# Integration tests only (requires Delta Lake, slower)
pytest -m integration

# Exclude slow tests
pytest -m "not slow"
```

### Run Specific Test Files

```bash
# Hash generation and core tests
pytest tests/test_type2_dimension.py

# Edge cases
pytest tests/test_type2_edge_cases.py

# Integration tests
pytest tests/test_type2_integration.py

# Type1 tests
pytest tests/test_type1_dimension.py
```

### Run Specific Tests

```bash
# Run single test
pytest tests/test_type2_dimension.py::test_hash_generation_basic

# Run tests matching pattern
pytest -k "hash_generation"
```

## Test Categories

### 1. Hash Generation Tests (test_type2_dimension.py)

Tests SHA-256 hash generation for change detection:

- ✅ `test_hash_generation_basic` - Verifies 64-char hex hash
- ✅ `test_hash_generation_consistency` - Identical data → same hash
- ✅ `test_hash_generation_sensitivity` - Different data → different hash

**Coverage:** Hash generation logic, SHA-256 correctness

### 2. Initialization Tests (test_type2_dimension.py)

Tests Type2Dimension constructor behavior:

- ✅ `test_init_with_empty_dimension` - Creates proper SCD2 schema
- ✅ `test_init_with_existing_dimension` - Uses provided DataFrame
- ✅ `test_init_stores_parameters_correctly` - Parameter assignment

**Coverage:** Initialization, schema creation, parameter handling

### 3. Edge Case Tests (test_type2_edge_cases.py)

Tests boundary conditions and error handling:

- ✅ `test_null_values_in_hash_columns` - NULL handling in concat_ws
- ✅ `test_empty_bronze_dataframe` - Empty dataset handling
- ✅ `test_special_characters_in_data` - Special chars (||, quotes, newlines)
- ✅ `test_unicode_data` - UTF-8 characters
- ✅ `test_very_wide_dataframe` - 100+ columns
- ✅ `test_concurrent_load_timestamps` - Identical timestamps
- ✅ `test_hash_columns_not_in_bronze` - Missing column error

**Coverage:** Edge cases, error conditions, data quality scenarios

### 4. Integration Tests (test_type2_integration.py)

Tests end-to-end SCD2 processing with Delta Lake:

- ✅ `test_bootstrap_empty_dimension_single_record` - Bootstrap with 1 record
- ✅ `test_bootstrap_empty_dimension_multiple_records` - Bootstrap with 5 records
- ✅ `test_update_single_changed_record` - SCD2 update (old expired, new version)
- ✅ `test_no_changes_returns_zero` - Unchanged records ignored
- ✅ `test_large_dataset_performance` - 1000 records (marked @slow)

**Coverage:** Delta merge operations, append operations, end-to-end workflows

### 5. Type1 Dimension Tests (test_type1_dimension.py)

Tests Type1Dimension (overwrite semantics):

- ✅ `test_type1_basic_update` - Update + insert logic
- ✅ `test_type1_outer_join_behavior` - Preserves all records
- ✅ `test_type1_no_matching_keys` - No overlapping keys
- ✅ `test_type1_empty_source` - Empty source handling
- ✅ `test_type1_empty_target` - Empty target handling

**Coverage:** Type1 processing, outer join logic

## Test Markers

Tests are tagged with pytest markers for selective execution:

- `@pytest.mark.unit` - Fast unit tests (no Delta operations)
- `@pytest.mark.integration` - Integration tests (require Delta Lake)
- `@pytest.mark.slow` - Long-running performance tests

## Fixtures

### SparkSession Fixtures (conftest.py)

- `spark_session` - Session-scoped SparkSession with Delta support
- `spark` - Function-scoped Spark (clears catalog between tests)
- `temp_delta_table` - Temporary Delta table with cleanup

### Data Fixtures (conftest.py)

- `sample_bronze_df` - Sample bronze DataFrame with LoadTimestamp
- `sample_dimension_df` - Sample dimension with SCD2 metadata
- `empty_dimension_schema` - Empty dimension schema
- `type2_processor_factory` - Factory to create Type2Dimension instances

### Helper Functions (helpers/data_generators.py)

- `create_bronze_data()` - Build bronze DataFrames
- `create_dimension_data()` - Build dimension DataFrames with SCD2 columns
- `create_scd2_chain()` - Create historical version chains
- `create_large_dataset()` - Generate large datasets for performance tests
- `create_bronze_with_changes()` - Create modified versions of data

### Custom Assertions (helpers/assertions.py)

- `assert_hash_valid()` - Validate SHA-256 hash format
- `assert_surrogate_keys_sequential()` - Verify SK auto-increment
- `assert_dataframe_count()` - Check record counts
- `assert_column_exists()` - Verify column presence
- `assert_no_null_values()` - Check for NULLs
- `assert_historical_chain_valid()` - Validate SCD2 temporal chain

## Coverage Goals

- **Target:** 90%+ coverage for SCD2.py
- **Target:** 95%+ coverage for SCD.py

### Generate Coverage Report

```bash
pytest --cov=. --cov-report=html --cov-report=term
```

View detailed coverage:
```bash
# Windows
start htmlcov/index.html

# Linux/Mac
open htmlcov/index.html
```

## CI/CD Integration

### GitHub Actions

Tests can be automated with GitHub Actions:

```yaml
name: Test DimensionProcessor
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements-dev.txt
      - run: pytest --cov=. --cov-report=xml -m "not slow"
      - uses: codecov/codecov-action@v3
```

## Known Issues

### Fixed Issues (Before Testing)

1. ✅ Missing SparkSession parameter (Issue #1)
2. ✅ Parameter typo: `primaryKeyColunmName` → `primaryKeyColumnName` (Issue #2)
3. ✅ Surrogate key generation bug (line 100)
4. ✅ String-based empty check → `Optional[DataFrame]` (Issue #3)

### Issues Discovered by Tests

Tests will expose:
- Issues #4-15 from GitHub issue tracker
- Performance bottlenecks (redundant count() calls)
- Missing input validation
- Edge case handling gaps

## Microsoft Fabric Integration

### Running Tests in Fabric Notebooks

While the test suite is designed for local development, you can also run tests in Microsoft Fabric Notebooks:

1. **Upload library and tests** to Fabric workspace
2. **Install pytest** in notebook:
   ```python
   %pip install pytest pytest-spark chispa
   ```
3. **Run tests** in notebook cell:
   ```python
   import pytest
   pytest.main(["-v", "tests/test_type2_dimension.py"])
   ```

### Fabric Integration Test Notebook

For end-to-end validation in Fabric, create a notebook with:

```python
# Cell 1: Import and setup
from SCD2 import Type2Dimension
import pyspark.sql.functions as F

# Cell 2: Create test data in Lakehouse
# Cell 3: Run ProcessUpdate
# Cell 4: Validate results with display()
# Cell 5: Cleanup
```

## Troubleshooting

### Test Failures

**"NameError: name 'spark' is not defined"**
- Ensure SCD2.py has been fixed with SparkSession injection
- Check that tests pass `spark` parameter to Type2Dimension

**"AnalysisException: Table not found"**
- Integration tests create temporary Delta tables
- Ensure tmp_path fixture is working
- Check that table cleanup is happening

**"ImportError: cannot import name Type2Dimension"**
- Ensure you're running from project root directory
- Check PYTHONPATH includes project root

### Performance Issues

If tests are slow:
- Run unit tests only: `pytest -m unit`
- Skip slow tests: `pytest -m "not slow"`
- Reduce dataset sizes in performance tests

## Contributing

When adding new tests:

1. **Follow naming convention:** `test_<feature>_<scenario>`
2. **Add docstrings:** Explain what the test validates
3. **Use appropriate markers:** `@pytest.mark.unit` or `@pytest.mark.integration`
4. **Use helpers:** Leverage `data_generators.py` and `assertions.py`
5. **Clean up resources:** Especially Delta tables in integration tests

## Further Reading

- [pytest documentation](https://docs.pytest.org/)
- [Delta Lake documentation](https://docs.delta.io/)
- [PySpark testing best practices](https://www.databricks.com/blog/2019/10/17/unit-testing-apache-spark-with-py-test.html)
- [Chispa DataFrame comparison](https://github.com/MrPowers/chispa)
