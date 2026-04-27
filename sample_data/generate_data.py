import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

# ── Reference data matching GMMCO's actual business ──────────
REGIONS = ['North', 'South', 'East', 'West', 'Central']
BRANCHES = [
    'Chennai', 'Mumbai', 'Delhi', 'Kolkata', 'Hyderabad',
    'Bangalore', 'Nagpur', 'Pune', 'Ahmedabad', 'Bhubaneswar'
]
SEGMENTS = ['Mining', 'Construction', 'Power & Energy', 'Oil & Gas', 'Infrastructure']
PRODUCTS = {
    'Mining': ['Hydraulic Mining Shovel', 'Off-Highway Truck', 'Dragline', 'Dozer'],
    'Construction': ['Backhoe Loader', 'Hydraulic Excavator', 'Motor Grader', 'Skid Steer Loader'],
    'Power & Energy': ['Diesel Generator', 'Gas Generator', 'Marine Engine', 'Industrial Engine'],
    'Oil & Gas': ['Petroleum Engine', 'Gas Compressor', 'Diesel Generator'],
    'Infrastructure': ['Backhoe Loader', 'Motor Grader', 'Hydraulic Excavator']
}
FAULT_CODES = {
    'Engine': ['E001-Overheating', 'E002-Oil Pressure Low', 'E003-Fuel System Fault'],
    'Hydraulics': ['H001-Pump Failure', 'H002-Cylinder Leak', 'H003-Filter Blocked'],
    'Transmission': ['T001-Gear Slip', 'T002-Clutch Wear', 'T003-Oil Leak'],
    'Electrical': ['L001-Battery Fault', 'L002-Sensor Failure', 'L003-Wiring Issue'],
    'Undercarriage': ['U001-Track Worn', 'U002-Sprocket Damage', 'U003-Roller Failure']
}

def random_date(start_days_ago=365, end_days_ago=0):
    days = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

def generate_serial():
    return f"CAT{random.randint(100000, 999999)}"

# ── 1. Equipment Sales ────────────────────────────────────────
sales_records = []
for i in range(1, 1001):
    segment = random.choice(SEGMENTS)
    product = random.choice(PRODUCTS[segment])
    base_price = {
        'Mining': random.randint(8000000, 50000000),
        'Construction': random.randint(1500000, 8000000),
        'Power & Energy': random.randint(500000, 5000000),
        'Oil & Gas': random.randint(3000000, 15000000),
        'Infrastructure': random.randint(2000000, 10000000)
    }[segment]

    sales_records.append({
        'sale_id': f'SALE{i:05d}',
        'date': random_date(),
        'branch': random.choice(BRANCHES),
        'region': random.choice(REGIONS),
        'customer_id': f'CUST{random.randint(1000, 1200):04d}',
        'customer_name': f'Customer_{random.randint(1000, 1200)}',
        'industry_segment': segment,
        'product_category': product,
        'model_code': f'{product[:3].upper()}-{random.randint(100, 999)}',
        'serial_number': generate_serial(),
        'sale_amount_inr': base_price + random.randint(-500000, 500000),
        'finance_type': random.choice(['Cash', 'Cat Finance', 'Bank Loan']),
        'salesperson_id': f'EMP{random.randint(100, 150):03d}'
    })

pd.DataFrame(sales_records).to_csv('sample_data/equipment_sales.csv', index=False)
print(f"Generated {len(sales_records)} sales records")

# ── 2. Parts Orders ───────────────────────────────────────────
parts_records = []
part_catalog = [
    ('P001', 'Engine Oil Filter', 450), ('P002', 'Hydraulic Pump Seal Kit', 8500),
    ('P003', 'Air Filter Element', 1200), ('P004', 'Track Link Assembly', 45000),
    ('P005', 'Fuel Injector', 18000), ('P006', 'Battery 12V', 6500),
    ('P007', 'Alternator', 22000), ('P008', 'Brake Lining Set', 9500),
    ('P009', 'Sprocket Segment', 38000), ('P010', 'Cutting Edge Blade', 12000)
]
warehouses = ['Chennai WH', 'Mumbai WH', 'Delhi WH', 'Nagpur WH']

for i in range(1, 2001):
    part_no, part_desc, unit_price = random.choice(part_catalog)
    qty = random.randint(1, 50)
    parts_records.append({
        'order_id': f'ORD{i:06d}',
        'date': random_date(),
        'part_number': part_no,
        'part_description': part_desc,
        'quantity_ordered': qty,
        'unit_price_inr': unit_price,
        'total_value_inr': qty * unit_price,
        'warehouse_location': random.choice(warehouses),
        'customer_id': f'CUST{random.randint(1000, 1200):04d}',
        'machine_serial': generate_serial(),
        'urgency_level': random.choice(['Standard', 'Urgent', 'Critical'])
    })

pd.DataFrame(parts_records).to_csv('sample_data/parts_orders.csv', index=False)
print(f"Generated {len(parts_records)} parts orders")

# ── 3. Service Work Orders ────────────────────────────────────
service_records = []
fault_categories = list(FAULT_CODES.keys())

for i in range(1, 1501):
    fault_cat = random.choice(fault_categories)
    fault_code = random.choice(FAULT_CODES[fault_cat])
    date_raised = random_date()
    sla_hours = random.choice([4, 8, 24, 48])
    actual_hours = sla_hours * random.uniform(0.5, 1.8)
    date_resolved = (datetime.strptime(date_raised, '%Y-%m-%d') +
                     timedelta(hours=actual_hours)).strftime('%Y-%m-%d')

    service_records.append({
        'workorder_id': f'WO{i:06d}',
        'date_raised': date_raised,
        'date_resolved': date_resolved,
        'machine_serial': generate_serial(),
        'fault_category': fault_cat,
        'fault_code': fault_code,
        'technician_id': f'TECH{random.randint(1, 80):03d}',
        'labour_hours': round(actual_hours, 1),
        'parts_cost_inr': random.randint(0, 50000),
        'sla_hours_promised': sla_hours,
        'sla_met': 'Yes' if actual_hours <= sla_hours else 'No',
        'branch': random.choice(BRANCHES),
        'region': random.choice(REGIONS),
        'customer_id': f'CUST{random.randint(1000, 1200):04d}'
    })

pd.DataFrame(service_records).to_csv('sample_data/service_workorders.csv', index=False)
print(f"Generated {len(service_records)} service workorders")

# ── 4. Customer Master ────────────────────────────────────────
customer_records = []
for i in range(1000, 1201):
    segment = random.choice(SEGMENTS)
    customer_records.append({
        'customer_id': f'CUST{i:04d}',
        'customer_name': f'Customer_{i}',
        'industry_segment': segment,
        'region': random.choice(REGIONS),
        'state': random.choice(['Tamil Nadu', 'Maharashtra', 'Odisha',
                                 'Jharkhand', 'Rajasthan', 'Gujarat',
                                 'West Bengal', 'Karnataka']),
        'fleet_size': random.randint(1, 150),
        'contract_type': random.choice(['CVA', 'Ad-hoc', 'Rental', 'None']),
        'contract_value_inr': random.randint(0, 5000000),
        'account_manager': f'EMP{random.randint(100, 150):03d}'
    })

pd.DataFrame(customer_records).to_csv('sample_data/customers.csv', index=False)
print(f"Generated {len(customer_records)} customers")
print("\nAll sample data generated successfully.")
