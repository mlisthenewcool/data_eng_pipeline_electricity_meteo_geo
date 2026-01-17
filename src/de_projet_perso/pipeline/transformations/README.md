# Dataset Transformations

This directory contains dataset-specific transformations for bronze and silver layers.

## 📁 Structure

```
transformations/
├── __init__.py                # Registry system (DO NOT MODIFY)
├── README.md                  # This file
├── ign_contours_iris.py       # IGN IRIS transformations
├── odre_installations.py      # ODRE installations transformations
└── [your_dataset].py          # Add your transformations here
```

## 🎯 How It Works

The transformation registry uses Python decorators to automatically register dataset-specific transformations:

1. **Bronze transformations** are applied after column normalization (snake_case)
2. **Silver transformations** are applied when creating the silver layer from bronze
3. If no transformation is registered, data passes through unchanged

## ✍️ Adding a New Transformation

### 1. Create a new file for your dataset

**File:** `transformations/my_dataset.py`

```python
"""Transformations for my_dataset."""

import polars as pl
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("my_dataset")
def transform_bronze_my_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Bronze transformation for my_dataset.
    
    Applied after column normalization.
    Use this for:
    - Filtering invalid rows
    - Basic data cleaning
    - Column selection
    
    Args:
        df: Input DataFrame from landing layer
        dataset_name: Dataset identifier
        
    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return df.filter(
        pl.col("status") == "active"
    )


@register_silver("my_dataset")
def transform_silver_my_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Silver transformation for my_dataset.
    
    Applied when creating silver layer.
    Use this for:
    - Type casting
    - Business logic
    - Calculated columns
    - Enrichment
    - Aggregations
    
    Args:
        df: Input DataFrame from bronze layer
        dataset_name: Dataset identifier
        
    Returns:
        Transformed DataFrame ready for silver layer
    """
    return df.with_columns([
        pl.col("amount").cast(pl.Float64),
        (pl.col("amount") * 1.2).alias("amount_with_tax"),
        pl.col("date").str.to_date(),
    ])
```

### 2. Register the module in `__init__.py`

Add your module to the import section:

```python
# In transformations/__init__.py, line ~90
try:
    from de_projet_perso.pipeline.transformations import (
        ign_contours_iris,
        odre_installations,
        my_dataset,  # ← Add your module here
    )
```

### 3. That's it!

The transformation will be automatically applied when processing the dataset.

## 🧪 Testing Your Transformation

```python
import polars as pl
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform

# Test bronze transformation
bronze_func = get_bronze_transform("my_dataset")
df = pl.DataFrame({"col": [1, 2, 3]})
result = bronze_func(df, "my_dataset")

# Test silver transformation
silver_func = get_silver_transform("my_dataset")
result = silver_func(df, "my_dataset")
```

## 📋 Best Practices

### ✅ DO
- Keep transformations simple and focused
- Handle missing columns gracefully
- Add clear docstrings
- Use type hints
- Test with sample data

### ❌ DON'T
- Don't modify the registry system (`__init__.py`)
- Don't use external API calls (use enrichment layer instead)
- Don't write files directly (return DataFrame only)
- Don't use blocking operations (keep it fast)

## 🔍 Examples

### Filtering Rows
```python
@register_bronze("my_dataset")
def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    return df.filter(pl.col("is_valid") == True)
```

### Type Casting
```python
@register_silver("my_dataset")
def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    return df.with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Int32),
    ])
```

### Adding Calculated Columns
```python
@register_silver("my_dataset")
def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    return df.with_columns([
        (pl.col("price") * pl.col("quantity")).alias("total"),
    ])
```

### Conditional Transformations
```python
@register_silver("my_dataset")
def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    # Check if column exists before transforming
    if "optional_column" in df.columns:
        return df.with_columns([
            pl.col("optional_column").cast(pl.Float64)
        ])
    return df
```

## 🐛 Troubleshooting

### My transformation is not being applied

1. **Check the dataset name** matches the catalog exactly
2. **Verify the module is imported** in `__init__.py`
3. **Check logs** for errors during registration
4. **Test the function directly** outside the pipeline

### I get a ColumnNotFoundError

Make sure to check if columns exist before using them:

```python
if "my_column" in df.columns:
    df = df.with_columns([...])
```

### The transformation works locally but not in Airflow

- Ensure the module is committed to git
- Check Airflow logs for import errors
- Verify the Python path is correct

## 📚 Resources

- [Polars documentation](https://pola-rs.github.io/polars/)
- [Polars expressions guide](https://pola-rs.github.io/polars/user-guide/expressions/)
- Project pipeline documentation: `../README.md`
