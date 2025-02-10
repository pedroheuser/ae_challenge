from src.etl.data_loader import load_all_data
from src.etl.data_quality import check_data_quality
import pandas as pd
from datetime import datetime


def analyze_ticket_medio(orders, order_details):
    order_details['total'] = order_details['unit_price'] * order_details['quantity'] * (1 - order_details['discount'])
    order_values = order_details.groupby('order_id')['total'].sum()
    return order_values.mean()

def analyze_churn(orders, window_days=90):
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    last_order_date = orders.groupby('customer_id')['order_date'].max()
    latest_date = orders['order_date'].max()
    churned = (latest_date - last_order_date).dt.days > window_days
    return (churned.sum() / len(churned)) * 100

def main():
    data = load_all_data()
    check_data_quality(data)
    ticket_medio = analyze_ticket_medio(data['orders'], data['order_details'])
    churn_rate = analyze_churn(data['orders'])
    print(f'Ticket Médio: R${ticket_medio:.2f}')
    print(f'Taxa de Churn: {churn_rate:.2f}%')


if __name__ == "__main__":
    main()