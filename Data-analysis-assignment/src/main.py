"""
Main script to run the data analyses.
"""

import sys
import os
from .superstore_analyzer import SuperstoreAnalyzer


def print_section(title: str):
    """Formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_subsection(title: str):
    """Formatted subsection header."""
    print(f"\n{'-' * 70}")
    print(f"  {title}")
    print("-" * 70)


def format_currency(value: float) -> str:
    return f"${value:,.2f}"


def run_all_analyses(csv_path: str):
    print_section("SUPERSTORE DATA ANALYSIS")
    print(f"\nAnalyzing data from: {csv_path}\n")
    
    analyzer = SuperstoreAnalyzer(csv_path)
    
    # 1. Basic Aggregations
    print_section("1. BASIC AGGREGATIONS")
    
    print_subsection("Total Sales")
    total_sales = analyzer.total_sales()
    print(f"Total Sales Revenue: {format_currency(total_sales)}")
    
    print_subsection("Total Profit")
    total_profit = analyzer.total_profit()
    print(f"Total Profit: {format_currency(total_profit)}")
    print(f"Profit Margin: {(total_profit / total_sales * 100):.2f}%")
    
    # 2. Grouping Operations
    print_section("2. GROUPING OPERATIONS")
    
    print_subsection("Sales by Category")
    sales_by_cat = analyzer.sales_by_category()
    for category, sales in sorted(sales_by_cat.items(), key=lambda x: x[1], reverse=True):
        print(f"  {category:20}: {format_currency(sales)}")
    
    print_subsection("Profit by Region")
    profit_by_reg = analyzer.profit_by_region()
    for region, profit in sorted(profit_by_reg.items(), key=lambda x: x[1], reverse=True):
        print(f"  {region:20}: {format_currency(profit)}")
    
    print_subsection("Sales by Customer Segment")
    sales_by_seg = analyzer.sales_by_segment()
    for segment, sales in sorted(sales_by_seg.items(), key=lambda x: x[1], reverse=True):
        print(f"  {segment:20}: {format_currency(sales)}")
    
    # 3. Streaming Statistics
    print_section("3. COMPREHENSIVE STATISTICS (Single Pass)")
    stats = analyzer.streaming_statistics()
    print(f"  Total Sales:        {format_currency(stats['total_sales'])}")
    print(f"  Total Profit:       {format_currency(stats['total_profit'])}")
    print(f"  Total Quantity:    {stats['total_quantity']:,}")
    print(f"  Record Count:       {stats['record_count']:,}")
    print(f"  Average Sale:       {format_currency(stats['avg_sale'])}")
    print(f"  Average Profit:     {format_currency(stats['avg_profit'])}")
    print(f"  Min Sale:           {format_currency(stats['min_sale'])}")
    print(f"  Max Sale:           {format_currency(stats['max_sale'])}")
    print(f"  Min Profit:         {format_currency(stats['min_profit'])}")
    print(f"  Max Profit:         {format_currency(stats['max_profit'])}")
    print(f"  Profit Margin:      {stats['profit_margin']:.2f}%")
    print(f"  Average Discount:   {stats['avg_discount']:.2%}")
    
    # 4. Filtering Operations
    print_section("4. FILTERING OPERATIONS")
    
    print_subsection("High-Value Orders (>= $1,000)")
    high_value_count = analyzer.high_value_orders(threshold=1000.0)
    print(f"Number of orders with sales >= $1,000: {high_value_count:,}")
    
    print_subsection("Profitable Orders Analysis")
    profitable = analyzer.profitable_orders()
    print(f"  Profitable Orders:     {profitable['count']:,}")
    print(f"  Total Sales:          {format_currency(profitable['total_sales'])}")
    print(f"  Total Profit:         {format_currency(profitable['total_profit'])}")
    print(f"  Average Profit:       {format_currency(profitable['avg_profit'])}")
    
    # 5. Top-K Analysis
    print_section("5. TOP-K ANALYSIS")
    
    print_subsection("Top 10 Products by Sales")
    top_products = analyzer.top_products_by_sales(k=10)
    for rank, (sales, product) in enumerate(top_products, 1):
        print(f"  {rank:2}. {product[:50]:50} : {format_currency(sales)}")
    
    print_subsection("Top 10 Customers by Lifetime Value")
    top_customers = analyzer.customer_lifetime_value(top_n=10)
    for rank, (sales, customer_id) in enumerate(top_customers, 1):
        print(f"  {rank:2}. {customer_id:20} : {format_currency(sales)}")
    
    # 6. Parallel Aggregations
    print_section("6. PARALLEL AGGREGATIONS (Using tee())")
    parallel = analyzer.parallel_aggregations()
    print(f"  Total Sales:              {format_currency(parallel['total_sales'])}")
    print(f"  Total Profit:             {format_currency(parallel['total_profit'])}")
    print(f"  Total Quantity:           {parallel['total_quantity']:,}")
    print(f"  Record Count:             {parallel['record_count']:,}")
    print(f"  Unique Customers:         {parallel['unique_customers']:,}")
    print(f"  Avg Sale per Record:      {format_currency(parallel['avg_sale_per_record'])}")
    print(f"  Avg Profit per Record:    {format_currency(parallel['avg_profit_per_record'])}")
    
    # 7. Nested Grouping
    print_section("7. NESTED GROUPING (Region -> Category)")
    nested = analyzer.sales_by_region_category()
    for region in sorted(nested.keys()):
        print(f"\n  {region}:")
        for category, sales in sorted(nested[region].items(), key=lambda x: x[1], reverse=True):
            print(f"    {category:20}: {format_currency(sales)}")
    
    # 8. Conditional Aggregation
    print_section("8. CONDITIONAL AGGREGATION")
    
    print_subsection("Electronics Sales")
    electronics_sales = analyzer.conditional_aggregate(
        lambda r: r['sales'],
        lambda r: r['category'] == 'Technology'
    )
    print(f"Total Technology Sales: {format_currency(electronics_sales)}")
    
    print_subsection("West Region Profit")
    west_profit = analyzer.conditional_aggregate(
        lambda r: r['profit'],
        lambda r: r['region'] == 'West'
    )
    print(f"Total West Region Profit: {format_currency(west_profit)}")
    
    # 9. Time Series Analysis
    print_section("9. TIME SERIES ANALYSIS")
    
    print_subsection("Monthly Sales Trend")
    monthly_trend = analyzer.monthly_sales_trend()
    for month, sales in list(monthly_trend.items())[:12]:  # Show first 12 months
        print(f"  {month}: {format_currency(sales)}")
    if len(monthly_trend) > 12:
        print(f"  ... and {len(monthly_trend) - 12} more months")
    
    print_section("ANALYSIS COMPLETE")
    print("\nAll analyses completed successfully!\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.main <path_to_superstore_csv>")
        print("\nExample: python3 -m src.main data/Superstore_data.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    
    try:
        run_all_analyses(csv_path)
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

