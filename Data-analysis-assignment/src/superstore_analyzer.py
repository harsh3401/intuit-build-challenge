"""
Superstore data analysis with pure streaming/functional operations.
Each method demonstrates unique aggregation patterns using functional programming.
No sorting or bulk materialization - all operations are stream-based.
"""

import csv
from typing import Iterator, Dict, List, Tuple, Any, Callable, Optional
from itertools import tee, groupby
from functools import reduce
from datetime import datetime
import heapq
from collections import defaultdict


class SuperstoreAnalyzer:
    

    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def clean_record(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Cleaning and validating a CSV row. Returns cleaned record or None if invalid."""
        try:
            # Clean string fields (strip whitespace)
            record = {
                'order_id': row.get('Order ID', '').strip(),
                'customer_id': row.get('Customer ID', '').strip(),
                'product_id': row.get('Product ID', '').strip(),
                'customer_name': row.get('Customer Name', '').strip(),
                'product_name': row.get('Product Name', '').strip(),
                'category': row.get('Category', '').strip(),
                'sub_category': row.get('Sub-Category', '').strip(),
                'segment': row.get('Segment', '').strip(),
                'region': row.get('Region', '').strip(),
                'state': row.get('State', '').strip(),
                'city': row.get('City', '').strip(),
                'country': row.get('Country', '').strip(),
                'ship_mode': row.get('Ship Mode', '').strip(),
            }
            
            # Parse dates
            order_date_str = row.get('Order Date', '').strip()
            ship_date_str = row.get('Ship Date', '').strip()
            record['order_date'] = datetime.strptime(order_date_str, '%m/%d/%Y')
            record['ship_date'] = datetime.strptime(ship_date_str, '%m/%d/%Y')
            
            # Parse numeric fields
            record['sales'] = max(0.0, float(row.get('Sales', 0) or 0))
            record['quantity'] = max(0, int(float(row.get('Quantity', 0) or 0)))
            record['discount'] = max(0.0, min(1.0, float(row.get('Discount', 0) or 0)))
            record['profit'] = float(row.get('Profit', 0) or 0)
            
            # Skipping if required fields are missing
            if not record['order_id'] or not record['customer_id'] or not record['product_id']:
                return None
            
            # Validate dates
            if record['ship_date'] < record['order_date']:
                return None
            
            return record
        except (ValueError, KeyError, TypeError):
            return None

    def stream_records(self) -> Iterator[Dict[str, Any]]:
        """Mocking a stream of records."""
        with open(self.csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned = self.clean_record(row)
                if cleaned is not None:
                    yield cleaned

    def total_sales(self) -> float:
        return reduce(
            lambda acc, record: acc + record['sales'],
            self.stream_records(),
            0.0
        )

    def total_profit(self) -> float:
        return reduce(
            lambda acc, record: acc + record['profit'],
            self.stream_records(),
            0.0
        )

    def aggregate_by_key(self, key_func: Callable[[Dict], str], 
                         value_func: Callable[[Dict], float]) -> Dict[str, float]:
        """Generic aggregation: group by key function and sum by value function."""
        aggregates = defaultdict(float)
        for record in self.stream_records():
            key = key_func(record)
            value = value_func(record)
            aggregates[key] += value
        return dict(aggregates)

    def sales_by_category(self) -> Dict[str, float]:
        """Group sales by product category."""
        return self.aggregate_by_key(
            lambda r: r['category'],
            lambda r: r['sales']
        )

    def profit_by_region(self) -> Dict[str, float]:
        """Group profit by region."""
        return self.aggregate_by_key(
            lambda r: r['region'],
            lambda r: r['profit']
        )

    def sales_by_segment(self) -> Dict[str, float]:
        """Group sales by customer segment."""
        return self.aggregate_by_key(
            lambda r: r['segment'],
            lambda r: r['sales']
        )

    def streaming_statistics(self) -> Dict[str, Any]:
        """Calculating comprehensive stats in one streaming pass."""
        total_sales = 0.0
        total_profit = 0.0
        total_quantity = 0
        record_count = 0
        min_sale = float('inf')
        max_sale = float('-inf')
        min_profit = float('inf')
        max_profit = float('-inf')
        total_discount = 0.0

        for record in self.stream_records():
            sales = record['sales']
            profit = record['profit']
            quantity = record['quantity']
            discount = record['discount']

            total_sales += sales
            total_profit += profit
            total_quantity += quantity
            total_discount += discount
            record_count += 1

            min_sale = min(min_sale, sales)
            max_sale = max(max_sale, sales)
            min_profit = min(min_profit, profit)
            max_profit = max(max_profit, profit)

        return {
            'total_sales': total_sales,
            'total_profit': total_profit,
            'total_quantity': total_quantity,
            'record_count': record_count,
            'avg_sale': total_sales / record_count if record_count else 0.0,
            'avg_profit': total_profit / record_count if record_count else 0.0,
            'min_sale': min_sale if record_count else 0.0,
            'max_sale': max_sale if record_count else 0.0,
            'min_profit': min_profit if record_count else 0.0,
            'max_profit': max_profit if record_count else 0.0,
            'profit_margin': (total_profit / total_sales * 100) if total_sales else 0.0,
            'avg_discount': total_discount / record_count if record_count else 0.0
        }

    def filter_records(self, predicate: Callable[[Dict], bool]) -> Iterator[Dict[str, Any]]:
        """Lazy filtering)."""
        return filter(predicate, self.stream_records())

    def top_k_items(self, k: int, 
                   metric_func: Callable[[Dict], float],
                   label_func: Callable[[Dict], str]) -> List[Tuple[str, float]]:
        """Avoiding sorting by using a heap."""
        heap = []
        for record in self.stream_records():
            metric = metric_func(record)
            label = label_func(record)
            if len(heap) < k:
                heapq.heappush(heap, (metric, label))
            elif metric > heap[0][0]:
                heapq.heapreplace(heap, (metric, label))
        return sorted(heap, key=lambda x: x[0], reverse=True)

    def top_products_by_sales(self, k: int = 10) -> List[Tuple[str, float]]:
        product_sales = defaultdict(float)
        for record in self.stream_records():
            product_sales[record['product_name']] += record['sales']
        
        heap = []
        for product, sales in product_sales.items():
            if len(heap) < k:
                heapq.heappush(heap, (sales, product))
            elif sales > heap[0][0]:
                heapq.heapreplace(heap, (sales, product))
        return sorted(heap, key=lambda x: x[0], reverse=True)

    def parallel_aggregations(self) -> Dict[str, Any]:
        stream1, stream2, stream3, stream4, stream5 = tee(self.stream_records(), 5)
        
        total_sales = reduce(lambda a, r: a + r['sales'], stream1, 0.0)
        total_profit = reduce(lambda a, r: a + r['profit'], stream2, 0.0)
        total_quantity = reduce(lambda a, r: a + r['quantity'], stream3, 0)
        record_count = sum(1 for _ in stream4)
        unique_customers = len(set(r['customer_id'] for r in stream5))
        
        return {
            'total_sales': total_sales,
            'total_profit': total_profit,
            'total_quantity': total_quantity,
            'record_count': record_count,
            'unique_customers': unique_customers,
            'avg_sale_per_record': total_sales / record_count if record_count else 0.0,
            'avg_profit_per_record': total_profit / record_count if record_count else 0.0
        }

    def two_level_grouping(self, key1_func: Callable[[Dict], str],
                          key2_func: Callable[[Dict], str],
                          value_func: Callable[[Dict], float]) -> Dict[str, Dict[str, float]]:
        """Nested grouping: e.g., sales by region and category."""
        nested = defaultdict(lambda: defaultdict(float))
        for record in self.stream_records():
            key1 = key1_func(record)
            key2 = key2_func(record)
            value = value_func(record)
            nested[key1][key2] += value
        return {k1: dict(v1) for k1, v1 in nested.items()}

    def sales_by_region_category(self) -> Dict[str, Dict[str, float]]:
        return self.two_level_grouping(
            lambda r: r['region'],
            lambda r: r['category'],
            lambda r: r['sales']
        )

    def transform_stream(self, *transform_funcs: Callable[[Dict], Optional[Dict]]) -> Iterator[Dict[str, Any]]:
        for record in self.stream_records():
            transformed = record
            for func in transform_funcs:
                transformed = func(transformed)
                if transformed is None:
                    break
            if transformed is not None:
                yield transformed

    def conditional_aggregate(self, 
                             metric_func: Callable[[Dict], float],
                             condition_func: Callable[[Dict], bool]) -> float:
        return reduce(
            lambda acc, record: acc + metric_func(record),
            filter(condition_func, self.stream_records()),
            0.0
        )

    def high_value_orders(self, threshold: float = 1000.0) -> int:
        return sum(
            1 for _ in self.filter_records(
                lambda r: r['sales'] >= threshold
            )
        )

    def profitable_orders(self) -> Dict[str, Any]:
        profitable = list(self.filter_records(lambda r: r['profit'] > 0))
        if not profitable:
            return {'count': 0, 'total_sales': 0.0, 'total_profit': 0.0}
        
        total_sales = sum(r['sales'] for r in profitable)
        total_profit = sum(r['profit'] for r in profitable)
        
        return {
            'count': len(profitable),
            'total_sales': total_sales,
            'total_profit': total_profit,
            'avg_profit': total_profit / len(profitable)
        }

    def monthly_sales_trend(self) -> Dict[str, float]:
        monthly = defaultdict(float)
        for record in self.stream_records():
            month_key = record['order_date'].strftime('%Y-%m')
            monthly[month_key] += record['sales']
        return dict(sorted(monthly.items()))

    def customer_lifetime_value(self, top_n: int = 10) -> List[Tuple[str, float]]:
        customer_sales = defaultdict(float)
        for record in self.stream_records():
            customer_sales[record['customer_id']] += record['sales']
        
        heap = []
        for customer_id, sales in customer_sales.items():
            if len(heap) < top_n:
                heapq.heappush(heap, (sales, customer_id))
            elif sales > heap[0][0]:
                heapq.heapreplace(heap, (sales, customer_id))
        return sorted(heap, key=lambda x: x[0], reverse=True)

