Superstore Data Analysis (Python)
==================================

A comprehensive data analysis application that demonstrates proficiency with functional programming, stream operations, data aggregation, and lambda expressions. The application analyzes the Superstore dataset using pure streaming operations.

Setup
-----

```bash
./scripts/setup_env.sh
source venv/bin/activate  # On Windows: venv\Scripts\activate
python3 -m src.main data/Superstore_data.csv
```

Running Tests
-------------

```bash
python3 -m unittest discover tests
```

Sample Output
-------------

```
======================================================================
  SUPERSTORE DATA ANALYSIS
======================================================================

Analyzing data from: data/Superstore_data.csv

======================================================================
  1. BASIC AGGREGATIONS
======================================================================

----------------------------------------------------------------------
  Total Sales
----------------------------------------------------------------------
Total Sales Revenue: $2,297,200.86

----------------------------------------------------------------------
  Total Profit
----------------------------------------------------------------------
Total Profit: $286,397.02
Profit Margin: 12.47%

======================================================================
  2. GROUPING OPERATIONS
======================================================================

----------------------------------------------------------------------
  Sales by Category
----------------------------------------------------------------------
  Furniture            : $741,999.80
  Office Supplies      : $719,047.03
  Technology           : $836,154.03

----------------------------------------------------------------------
  Profit by Region
----------------------------------------------------------------------
  West                 : $108,418.50
  East                 : $91,519.60
  Central              : $39,770.30
  South                : $46,688.62

======================================================================
  3. COMPREHENSIVE STATISTICS (Single Pass)
======================================================================
  Total Sales:        $2,297,200.86
  Total Profit:       $286,397.02
  Total Quantity:     37,873
  Record Count:       9,994
  Average Sale:       $229.86
  Average Profit:     $28.66
  Min Sale:           $0.44
  Max Sale:           $22,638.48
  Profit Margin:      12.47%

======================================================================
  4. FILTERING OPERATIONS
======================================================================

----------------------------------------------------------------------
  High-Value Orders (>= $1,000)
----------------------------------------------------------------------
Number of orders with sales >= $1,000: 1,234

======================================================================
  5. TOP-K ANALYSIS
======================================================================

----------------------------------------------------------------------
  Top 10 Products by Sales
----------------------------------------------------------------------
   1. Canon imageCLASS 2200 Advanced Copier : $61,599.83
   2. Canon imageCLASS 2200 Advanced Copier : $61,599.83
   3. Hewlett-Packard LaserJet 3310 Copier  : $27,419.98
   ...

======================================================================
  6. PARALLEL AGGREGATIONS (Using tee())
======================================================================
  Total Sales:              $2,297,200.86
  Total Profit:             $286,397.02
  Total Quantity:           37,873
  Record Count:             9,994
  Unique Customers:         793
  Avg Sale per Record:      $229.86
  Avg Profit per Record:    $28.66
```

Key Features
------------

- **Streaming Operations**: Process large CSV files without loading everything into memory
- **Functional Programming**: Uses reduce, filter, lambda expressions, and generators
- **Data Cleaning & Validation**: Automatic data cleaning and validation
- **Single-Pass Statistics**: Calculate multiple metrics in one streaming pass
- **Heap Optimization**: Top-K analysis without full sorting
- **Parallel Aggregations**: Multiple metrics using `tee()` for efficiency
- **Lazy Evaluation**: Filtering and transformations are lazy (computed on demand)

Technical Highlights
--------------------

- **Reduce Operations**: Aggregations using `functools.reduce`
- **Generator Functions**: `yield` for memory-efficient streaming
- **Defaultdict Aggregation**: Efficient grouping without pre-initialization
- **Heap-based Top-K**: O(n log k) instead of O(n log n) for sorting
- **Stream Tee**: Parallel processing of the same stream

Project Structure
-----------------

```
Data-analysis-assignment/
├── data/
│   └── Superstore_data.csv
├── src/
│   ├── superstore_analyzer.py    # Core analysis class
│   └── main.py                   # Main execution script
├── scripts/
│   └── setup_env.sh             # Virtual environment setup script
├── tests/
│   └── test_superstore_analyzer.py
├── requirements.txt
└── README.md
```

Dependencies
------------

**No external dependencies required!** Uses only Python standard library:
- `csv` for file reading
- `functools.reduce` for aggregations
- `itertools.tee` for parallel streams
- `heapq` for top-K optimization
- `collections.defaultdict` for grouping
