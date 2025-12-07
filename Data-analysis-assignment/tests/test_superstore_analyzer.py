import csv
import unittest
from pathlib import Path
import tempfile
import os
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from superstore_analyzer import SuperstoreAnalyzer


def create_test_csv(rows: list) -> str:
    fd, filepath = tempfile.mkstemp(suffix='.csv', text=True)
    try:
        with os.fdopen(fd, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Order ID', 'Order Date', 'Ship Date', 'Ship Mode',
                'Customer ID', 'Customer Name', 'Segment', 'Country',
                'City', 'State', 'Region', 'Product ID', 'Category',
                'Sub-Category', 'Product Name', 'Sales', 'Quantity',
                'Discount', 'Profit'
            ])
            writer.writerows(rows)
        return filepath
    except Exception:
        os.close(fd)
        raise


class TestSuperstoreAnalyzer(unittest.TestCase):

    def setUp(self):
        self.rows = [
            ['ORD1', '01/01/2024', '01/05/2024', 'Standard', 'C1', 'John', 'Consumer',
             'USA', 'NYC', 'NY', 'East', 'P1', 'Furniture', 'Chairs', 'Chair', '100', '2', '0.1', '20'],
            ['ORD2', '01/02/2024', '01/06/2024', 'Standard', 'C2', 'Jane', 'Corporate',
             'USA', 'LA', 'CA', 'West', 'P2', 'Electronics', 'Phones', 'Phone', '200', '1', '0.0', '50'],
            ['ORD3', '01/03/2024', '01/07/2024', 'Standard', 'C3', 'Bob', 'Home Office',
             'USA', 'NYC', 'NY', 'East', 'P3', 'Furniture', 'Tables', 'Table', '150', '1', '0.0', '30'],
            ['ORD4', '02/01/2024', '02/05/2024', 'Express', 'C1', 'John', 'Consumer',
             'USA', 'NYC', 'NY', 'East', 'P4', 'Office Supplies', 'Paper', 'Paper', '500', '3', '0.2', '100'],
        ]
        self.filepath = create_test_csv(self.rows)

    def tearDown(self):
        os.unlink(self.filepath)

    def test_total_sales(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        self.assertAlmostEqual(analyzer.total_sales(), 950.0, places=2)

    def test_total_profit(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        self.assertAlmostEqual(analyzer.total_profit(), 200.0, places=2)

    def test_sales_by_category(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.sales_by_category()
        self.assertAlmostEqual(result['Furniture'], 250.0, places=2)
        self.assertAlmostEqual(result['Electronics'], 200.0, places=2)
        self.assertAlmostEqual(result['Office Supplies'], 500.0, places=2)

    def test_profit_by_region(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.profit_by_region()
        self.assertAlmostEqual(result['East'], 150.0, places=2)
        self.assertAlmostEqual(result['West'], 50.0, places=2)

    def test_sales_by_segment(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.sales_by_segment()
        self.assertAlmostEqual(result['Consumer'], 600.0, places=2)
        self.assertAlmostEqual(result['Corporate'], 200.0, places=2)
        self.assertAlmostEqual(result['Home Office'], 150.0, places=2)

    def test_streaming_statistics(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        stats = analyzer.streaming_statistics()
        self.assertAlmostEqual(stats['total_sales'], 950.0, places=2)
        self.assertAlmostEqual(stats['total_profit'], 200.0, places=2)
        self.assertEqual(stats['total_quantity'], 7)
        self.assertEqual(stats['record_count'], 4)
        self.assertAlmostEqual(stats['min_sale'], 100.0, places=2)
        self.assertAlmostEqual(stats['max_sale'], 500.0, places=2)
        self.assertAlmostEqual(stats['avg_sale'], 237.5, places=2)
        self.assertAlmostEqual(stats['avg_profit'], 50.0, places=2)
        self.assertAlmostEqual(stats['min_profit'], 20.0, places=2)
        self.assertAlmostEqual(stats['max_profit'], 100.0, places=2)
        self.assertAlmostEqual(stats['profit_margin'], 21.05, places=2)
        self.assertAlmostEqual(stats['avg_discount'], 0.075, places=3)

    def test_filter_records(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        filtered = list(analyzer.filter_records(lambda r: r['sales'] > 150))
        self.assertEqual(len(filtered), 2)
        self.assertTrue(all(r['sales'] > 150 for r in filtered))

    def test_top_k_items(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.top_k_items(2, lambda r: r['sales'], lambda r: r['product_name'])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][1], 'Paper')
        self.assertAlmostEqual(result[0][0], 500.0, places=2)

    def test_top_products_by_sales(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.top_products_by_sales(2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][1], 'Paper')
        self.assertAlmostEqual(result[0][0], 500.0, places=2)

    def test_parallel_aggregations(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.parallel_aggregations()
        self.assertAlmostEqual(result['total_sales'], 950.0, places=2)
        self.assertAlmostEqual(result['total_profit'], 200.0, places=2)
        self.assertEqual(result['total_quantity'], 7)
        self.assertEqual(result['record_count'], 4)
        self.assertEqual(result['unique_customers'], 3)
        self.assertAlmostEqual(result['avg_sale_per_record'], 237.5, places=2)
        self.assertAlmostEqual(result['avg_profit_per_record'], 50.0, places=2)

    def test_sales_by_region_category(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.sales_by_region_category()
        self.assertAlmostEqual(result['East']['Furniture'], 250.0, places=2)
        self.assertAlmostEqual(result['East']['Office Supplies'], 500.0, places=2)
        self.assertAlmostEqual(result['West']['Electronics'], 200.0, places=2)

    def test_conditional_aggregate(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.conditional_aggregate(
            lambda r: r['sales'],
            lambda r: r['profit'] > 30
        )
        self.assertAlmostEqual(result, 700.0, places=2)

    def test_high_value_orders(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        self.assertEqual(analyzer.high_value_orders(threshold=150), 3)
        self.assertEqual(analyzer.high_value_orders(threshold=1000), 0)

    def test_profitable_orders(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.profitable_orders()
        self.assertEqual(result['count'], 4)
        self.assertAlmostEqual(result['total_sales'], 950.0, places=2)
        self.assertAlmostEqual(result['total_profit'], 200.0, places=2)
        self.assertAlmostEqual(result['avg_profit'], 50.0, places=2)

    def test_monthly_sales_trend(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.monthly_sales_trend()
        self.assertAlmostEqual(result['2024-01'], 450.0, places=2)
        self.assertAlmostEqual(result['2024-02'], 500.0, places=2)

    def test_customer_lifetime_value(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        result = analyzer.customer_lifetime_value(top_n=2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][1], 'C1')
        self.assertAlmostEqual(result[0][0], 600.0, places=2)

    def test_clean_record_invalid(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        invalid_row = {'Order ID': '', 'Customer ID': 'C1', 'Product ID': 'P1',
                      'Order Date': '01/01/2024', 'Ship Date': '01/05/2024',
                      'Sales': '100', 'Quantity': '1', 'Discount': '0', 'Profit': '10'}
        self.assertIsNone(analyzer.clean_record(invalid_row))

    def test_clean_record_invalid_date(self):
        analyzer = SuperstoreAnalyzer(self.filepath)
        invalid_row = {'Order ID': 'ORD1', 'Customer ID': 'C1', 'Product ID': 'P1',
                      'Order Date': '01/05/2024', 'Ship Date': '01/01/2024',
                      'Sales': '100', 'Quantity': '1', 'Discount': '0', 'Profit': '10'}
        self.assertIsNone(analyzer.clean_record(invalid_row))

    def test_empty_file(self):
        empty_file = create_test_csv([])
        try:
            analyzer = SuperstoreAnalyzer(empty_file)
            self.assertEqual(analyzer.total_sales(), 0.0)
            self.assertEqual(analyzer.streaming_statistics()['record_count'], 0)
        finally:
            os.unlink(empty_file)


if __name__ == '__main__':
    unittest.main()
