"""
Dataset Generator
=================
Generates sample sales data for testing the query system.

Features:
- Generate realistic sales data
- Configurable size
- Multiple regions, products, years
- Export to CSV
- https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


class DatasetGenerator:
    """
    Generates sample sales dataset for query processing.
    """
    
    def __init__(self, num_records: int = 10000):
        """
        Initialize the generator.
        
        Args:
            num_records: Number of records to generate
        """
        self.num_records = num_records
        
        # Data options
        self.products = [
            'Laptop', 'Desktop', 'Tablet', 'Smartphone', 'Monitor',
            'Keyboard', 'Mouse', 'Printer', 'Scanner', 'Webcam',
            'Headphones', 'Speakers', 'Router', 'Modem', 'Hard Drive'
        ]
        
        self.regions = ['North', 'South', 'East', 'West', 'Central']
        
        self.categories = [
            'Electronics', 'Computers', 'Accessories', 
            'Networking', 'Storage', 'Audio'
        ]
        
        self.customers = [f'Customer_{i:04d}' for i in range(1, 501)]
        
        self.years = [2021, 2022, 2023, 2024]
        self.months = list(range(1, 13))
    
    def generate_data(self) -> pd.DataFrame:
        """
        Generate the complete dataset.
        
        Returns:
            DataFrame with generated sales data
        """
        data = []
        
        for _ in range(self.num_records):
            # Random selections
            product = random.choice(self.products)
            region = random.choice(self.regions)
            category = random.choice(self.categories)
            customer = random.choice(self.customers)
            year = random.choice(self.years)
            month = random.choice(self.months)
            
            # Generate realistic quantities and prices
            quantity = random.randint(1, 100)
            
            # Base price depends on product
            base_prices = {
                'Laptop': 800, 'Desktop': 1000, 'Tablet': 400,
                'Smartphone': 600, 'Monitor': 300, 'Keyboard': 50,
                'Mouse': 30, 'Printer': 200, 'Scanner': 150,
                'Webcam': 80, 'Headphones': 100, 'Speakers': 150,
                'Router': 100, 'Modem': 80, 'Hard Drive': 120
            }
            
            unit_price = base_prices.get(product, 100) * random.uniform(0.8, 1.2)
            
            # Calculate values
            revenue = quantity * unit_price
            cost = revenue * random.uniform(0.5, 0.7)
            profit = revenue - cost
            sales = revenue  # alias for revenue
            
            # Create record
            record = {
                'transaction_id': f'TXN{_:06d}',
                'product': product,
                'category': category,
                'region': region,
                'customer': customer,
                'year': year,
                'month': month,
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'revenue': round(revenue, 2),
                'cost': round(cost, 2),
                'profit': round(profit, 2),
                'sales': round(sales, 2)
            }
            
            data.append(record)
        
        df = pd.DataFrame(data)
        
        # Sort by year and month
        df = df.sort_values(['year', 'month']).reset_index(drop=True)
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = 'sales_data.csv'):
        """
        Save the dataset to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
        """
        df.to_csv(filename, index=False)
        print(f"Dataset saved to {filename}")
        print(f"Total records: {len(df)}")
        print(f"File size: {len(df) * 100 / 1024:.2f} KB (approximate)")
    
    def get_data_summary(self, df: pd.DataFrame) -> str:
        """
        Get a summary of the generated dataset.
        
        Args:
            df: DataFrame to summarize
            
        Returns:
            Summary string
        """
        summary = "Dataset Summary\n"
        summary += "=" * 70 + "\n\n"
        
        summary += f"Total Records: {len(df):,}\n"
        summary += f"Date Range: {df['year'].min()} - {df['year'].max()}\n\n"
        
        summary += f"Products: {df['product'].nunique()} unique ({', '.join(df['product'].unique()[:5])}...)\n"
        summary += f"Regions: {df['region'].nunique()} unique ({', '.join(df['region'].unique())})\n"
        summary += f"Categories: {df['category'].nunique()} unique ({', '.join(df['category'].unique())})\n"
        summary += f"Customers: {df['customer'].nunique()} unique\n\n"
        
        summary += "Financial Summary:\n"
        summary += f"  Total Revenue: ${df['revenue'].sum():,.2f}\n"
        summary += f"  Total Profit: ${df['profit'].sum():,.2f}\n"
        summary += f"  Total Quantity Sold: {df['quantity'].sum():,}\n"
        summary += f"  Average Transaction: ${df['revenue'].mean():,.2f}\n\n"
        
        summary += "Sample Records:\n"
        summary += "-" * 70 + "\n"
        summary += df.head(5).to_string(index=False)
        
        return summary


# Example usage
if __name__ == '__main__':
    # Generate dataset
    generator = DatasetGenerator(num_records=10000)
    df = generator.generate_data()
    
    # Show summary
    print(generator.get_data_summary(df))
    
    # Save to CSV
    generator.save_to_csv(df, 'data/sales_data.csv')