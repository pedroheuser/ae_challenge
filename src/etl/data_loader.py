import pandas as pd
import os


def load_all_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    data_dir = os.path.join(project_root, 'data')

    return {
        'orders': pd.read_csv(os.path.join(data_dir, 'orders.csv'), delimiter=';'),
        'order_details': pd.read_csv(os.path.join(data_dir, 'order_details.csv'), delimiter=';'),
        'products': pd.read_csv(os.path.join(data_dir, 'products.csv'), delimiter=';'),
        'customers': pd.read_csv(os.path.join(data_dir, 'customers.csv'), delimiter=';'),
        'employees': pd.read_csv(os.path.join(data_dir, 'employees.csv'), delimiter=';'),
        'categories': pd.read_csv(os.path.join(data_dir, 'categories.csv'), delimiter=';'),
        'suppliers': pd.read_csv(os.path.join(data_dir, 'suppliers.csv'), delimiter=';'),
        'shippers': pd.read_csv(os.path.join(data_dir, 'shippers.csv'), delimiter=';'),
        'territories': pd.read_csv(os.path.join(data_dir, 'territories.csv'), delimiter=';'),
        'region': pd.read_csv(os.path.join(data_dir, 'region.csv'), delimiter=';'),
        'us_states': pd.read_csv(os.path.join(data_dir, 'us_states.csv'), delimiter=';'),
        'employee_territories': pd.read_csv(os.path.join(data_dir, 'employee_territories.csv'), delimiter=';')
    }