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


def analyze_sales_performance(data):
    order_details = data['order_details']
    products = data['products']
    categories = data['categories']
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])
    sales_by_product = (order_details
                        .merge(products, on='product_id')
                        .merge(categories, on='category_id')
                        .groupby(['category_name', 'product_name'])
                        .agg({
        'total_sale': 'sum',
        'quantity': 'sum'
    })
                        .sort_values('total_sale', ascending=False)
                        )

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE DE VENDAS")
    print("=" * 50 + "\n")
    print("Top 10 Produtos por Receita:")
    print(sales_by_product.head(10))
    category_sales = sales_by_product.groupby('category_name').sum()
    print("\nVendas por Categoria:")
    print(category_sales.sort_values('total_sale', ascending=False))

    return sales_by_product


def analyze_active_vs_inactive_products(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    products = data['products'].copy()
    categories = data['categories'].copy()

    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    sales_data = (order_details
                  .merge(products, on='product_id')
                  .merge(categories, on='category_id')
                  .merge(orders, on='order_id'))

    sales_data['order_date'] = pd.to_datetime(sales_data['order_date'])
    sales_data['year_month'] = sales_data['order_date'].dt.to_period('M')

    for status in [0, 1]:
        status_name = "ATIVOS" if status == 0 else "INATIVOS"
        df_status = sales_data.loc[sales_data['discontinued'] == status].copy()

        print("\n" + "=" * 50)
        print(f" ANÁLISE DE PRODUTOS {status_name}")
        print("=" * 50)

        print("\n1. Métricas Gerais:")
        print(f"Total de produtos: {len(df_status['product_id'].unique())}")
        print(f"Total de vendas: R$ {df_status['total_sale'].sum():,.2f}")
        print(f"Ticket médio: R$ {df_status['total_sale'].mean():,.2f}")
        print(f"Quantidade total vendida: {df_status['quantity'].sum():,}")

        print("\n2. Top 5 Produtos por Receita:")
        top_products = (df_status.groupby('product_name')
                        .agg({
            'total_sale': 'sum',
            'quantity': 'sum'
        })
                        .sort_values('total_sale', ascending=False)
                        .head())
        print(top_products)

        print("\n3. Vendas por Categoria:")
        category_analysis = (df_status.groupby('category_name')
                             .agg({
            'total_sale': 'sum',
            'quantity': 'sum',
            'product_id': 'nunique'
        })
                             .round(2)
                             .sort_values('total_sale', ascending=False))
        print(category_analysis)

        print("\n4. Tendência de Vendas por Ano-Mês:")
        temporal_analysis = (df_status.groupby('year_month')
                             .agg({
            'total_sale': 'sum',
            'order_id': 'nunique'
        })
                             .tail())
        print(temporal_analysis)

        print("\n5. Impacto dos Descontos:")
        discount_analysis = (df_status.groupby('discount')
                             .agg({
            'total_sale': 'sum',
            'quantity': 'sum'
        })
                             .sort_values('total_sale', ascending=False))
        print(discount_analysis)

    print("\n" + "=" * 50)
    print(" COMPARATIVO FINAL ATIVOS VS INATIVOS")
    print("=" * 50)

    comparative = pd.DataFrame({
        'Métrica': ['Total Produtos', 'Total Vendas', 'Ticket Médio', 'Qtd Total'],
        'Ativos': [
            len(sales_data[sales_data['discontinued'] == 0]['product_id'].unique()),
            sales_data[sales_data['discontinued'] == 0]['total_sale'].sum(),
            sales_data[sales_data['discontinued'] == 0]['total_sale'].mean(),
            sales_data[sales_data['discontinued'] == 0]['quantity'].sum()
        ],
        'Inativos': [
            len(sales_data[sales_data['discontinued'] == 1]['product_id'].unique()),
            sales_data[sales_data['discontinued'] == 1]['total_sale'].sum(),
            sales_data[sales_data['discontinued'] == 1]['total_sale'].mean(),
            sales_data[sales_data['discontinued'] == 1]['quantity'].sum()
        ]
    })

    comparative['Ativos'] = comparative.apply(
        lambda x: f"{x['Ativos']:,.2f}" if isinstance(x['Ativos'], (float, int)) else x['Ativos'], axis=1)
    comparative['Inativos'] = comparative.apply(
        lambda x: f"{x['Inativos']:,.2f}" if isinstance(x['Inativos'], (float, int)) else x['Inativos'], axis=1)

    print("\nComparativo Final:")
    print(comparative.to_string(index=False))

def analyze_product_status(data):
    order_details = data['order_details'].copy()
    products = data['products'].copy()

    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    sales_by_status = (order_details
    .merge(products, on='product_id')
    .groupby('discontinued')
    .agg({
        'total_sale': 'sum',
        'quantity': 'sum'
    }))

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE POR STATUS DO PRODUTO")
    print("=" * 50 + "\n")
    print("Vendas por Status do Produto:")
    print("\nProdutos Ativos (0):")
    if 0 in sales_by_status.index:
        print(f"Total vendas: R$ {sales_by_status.loc[0, 'total_sale']:,.2f}")
        print(f"Quantidade vendida: {sales_by_status.loc[0, 'quantity']:,}")

    print("\nProdutos Inativos (1):")
    if 1 in sales_by_status.index:
        print(f"Total vendas: R$ {sales_by_status.loc[1, 'total_sale']:,.2f}")
        print(f"Quantidade vendida: {sales_by_status.loc[1, 'quantity']:,}")

def analyze_seasonality(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    orders['month'] = orders['order_date'].dt.month
    orders['year'] = orders['order_date'].dt.year
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    sales_by_month = (order_details
                      .merge(orders, on='order_id')
                      .groupby(['year', 'month'])
                      ['total_sale'].sum()
                      .reset_index()
                      )

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE DE SAZONALIDADE")
    print("=" * 50 + "\n")
    print("Vendas por Mês:")
    print(sales_by_month.sort_values('total_sale', ascending=False))

    return sales_by_month


def analyze_geographic_distribution(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])
    sales_by_country = (order_details
                        .merge(orders, on='order_id')
                        .groupby('ship_country')
                        .agg({
        'total_sale': 'sum',
        'order_id': 'count'
    })
                        .rename(columns={'order_id': 'total_orders'})
                        .sort_values('total_sale', ascending=False)
                        )

    print("\n" + "=" * 50)
    print(" " * 10 + "ANÁLISE DE DISTRIBUIÇÃO GEOGRÁFICA")
    print("=" * 50 + "\n")
    print("Vendas por País:")
    print(sales_by_country)

    return sales_by_country


def analyze_cross_selling(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    products = data['products'].copy()

    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])
    discount_analysis = order_details.groupby('discount').agg({
        'quantity': 'sum',
        'total_sale': 'sum'
    }).sort_values('total_sale', ascending=False)
    products = products[['product_id', 'product_name']]  # Simplifica para evitar duplicação
    order_pairs = (order_details.merge(order_details, on='order_id')
                   .query('product_id_x < product_id_y')
                   .merge(products, left_on='product_id_x', right_on='product_id', suffixes=('', '_x'))
                   .merge(products, left_on='product_id_y', right_on='product_id', suffixes=('', '_y')))

    frequent_pairs = (order_pairs.groupby(['product_name', 'product_name_y'])
                      .size()
                      .sort_values(ascending=False))

    pairs_df = pd.DataFrame(frequent_pairs).reset_index()
    pairs_df.columns = ['Produto 1', 'Produto 2', 'Frequência']

    print("\nImpacto dos Descontos:")
    print(discount_analysis)

    print("\nTop 10 Pares de Produtos:")
    print(pairs_df.head(10).to_string(index=False))

    return discount_analysis, frequent_pairs


def analyze_customer_behavior(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    customers = data['customers'].copy()
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    customer_frequency = orders.groupby('customer_id').agg({
        'order_id': 'count',
        'order_date': ['min', 'max']
    })

    freq_stats = customer_frequency['order_id']['count'].describe().round(2)

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE DE CLIENTES")
    print("=" * 50)

    print("\nPerfil de Compras dos Clientes:")
    print(f"\nTotal de clientes ativos: {freq_stats['count']:.0f}")
    print(f"Número médio de pedidos por cliente: {freq_stats['mean']:.1f}")
    print("\nSegmentação por frequência de compra:")
    print(
        f"- Clientes de baixa frequência (1-4 pedidos): {len(customer_frequency[customer_frequency['order_id']['count'] <= 4])} clientes")
    print(
        f"- Clientes de média frequência (5-12 pedidos): {len(customer_frequency[(customer_frequency['order_id']['count'] > 4) & (customer_frequency['order_id']['count'] <= 12)])} clientes")
    print(
        f"- Clientes de alta frequência (>12 pedidos): {len(customer_frequency[customer_frequency['order_id']['count'] > 12])} clientes")
    print(f"\nCliente mais frequente: {freq_stats['max']:.0f} pedidos")

    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])
    customer_value = (order_details.merge(orders, on='order_id')
                      .merge(customers[['customer_id', 'company_name']], on='customer_id')
                      .groupby(['customer_id', 'company_name'])
                      .agg({'total_sale': 'sum'})
                      .round(2))

    print("\nTop 10 Clientes por Valor Total de Compras:")
    top_customers = customer_value.sort_values('total_sale', ascending=False).head(10)
    print(top_customers.to_string(float_format=lambda x: f"R$ {x:,.2f}"))


def analyze_customer_patterns(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])
    order_values = order_details.groupby('order_id')['total_sale'].sum()
    orders_with_values = orders.merge(order_values.reset_index(), on='order_id')

    orders['order_date'] = pd.to_datetime(orders['order_date'])
    customer_orders = orders.groupby('customer_id').agg({
        'order_id': 'count',
        'order_date': lambda x: (x.max() - x.min()).days
    }).rename(columns={'order_date': 'customer_lifetime_days'})

    customer_orders['segment'] = pd.cut(customer_orders['order_id'],
                                        bins=[0, 4, 12, float('inf')],
                                        labels=['Baixa', 'Média', 'Alta'])

    last_order_date = orders.groupby('customer_id')['order_date'].max()
    last_order_overall = orders['order_date'].max()
    days_since_last = (last_order_overall - last_order_date).dt.days

    churned_customers = days_since_last[days_since_last > 90]

    churned_values = (orders_with_values[orders_with_values['customer_id'].isin(churned_customers.index)]
    .groupby('customer_id')
    .agg({
        'total_sale': ['count', 'mean', 'sum']
    }))

    print("\n" + "=" * 50)
    print(" " * 15 + "PADRÕES DE CLIENTES")
    print("=" * 50)

    print("\nPadrões por Segmento:")
    segment_stats = customer_orders.groupby('segment').agg({
        'order_id': ['count', 'mean'],
        'customer_lifetime_days': 'mean'
    }).round(1)
    print(segment_stats)

    print("\nAnálise de Clientes Inativos (>90 dias):")
    print(f"Total de clientes inativos: {len(churned_customers)}")
    print(f"Valor médio por pedido dos inativos: R$ {churned_values['total_sale']['mean'].mean():.2f}")
    print(f"Valor total perdido: R$ {churned_values['total_sale']['sum'].sum():.2f}")


def analyze_temporal_patterns(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    products = data['products'].copy()
    categories = data['categories'].copy()

    orders['order_date'] = pd.to_datetime(orders['order_date'])
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    sales_data = (order_details.merge(orders, on='order_id')
                  .merge(products, on='product_id')
                  .merge(categories, on='category_id'))

    sales_data['year_month'] = sales_data['order_date'].dt.to_period('M')

    monthly_metrics = sales_data.groupby('year_month').agg({
        'total_sale': ['sum', 'mean'],
        'order_id': 'nunique',
        'customer_id': 'nunique'
    }).round(2)

    category_monthly = (sales_data.groupby(['year_month', 'category_name'])['total_sale']
                        .sum()
                        .unstack()
                        .fillna(0))

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE TEMPORAL")
    print("=" * 50)

    print("\nMétricas Mensais:")
    print(monthly_metrics)

    print("\nTop 3 Meses por Receita:")
    top_months = monthly_metrics.sort_values(('total_sale', 'sum'), ascending=False).head(3)
    print(top_months)

    print("\nCrescimento Mês a Mês (%):")
    monthly_growth = monthly_metrics[('total_sale', 'sum')].pct_change() * 100
    print(monthly_growth.tail().round(2))


def analyze_category_seasonality(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()
    products = data['products'].copy()
    categories = data['categories'].copy()

    orders['order_date'] = pd.to_datetime(orders['order_date'])
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    sales_data = (order_details.merge(orders, on='order_id')
                  .merge(products, on='product_id')
                  .merge(categories, on='category_id'))

    sales_data['month'] = sales_data['order_date'].dt.month

    category_season = (sales_data.groupby(['category_name', 'month'])
                       .agg({
        'total_sale': 'sum',
        'quantity': 'sum',
        'discount': 'mean'
    })
                       .round(2))

    discount_impact = (sales_data.groupby('category_name')
                       .agg({
        'discount': 'mean',
        'total_sale': 'sum',
        'quantity': 'sum'
    })
                       .sort_values('total_sale', ascending=False)
                       .round(2))

    print("\n" + "=" * 50)
    print(" " * 10 + "ANÁLISE DE SAZONALIDADE E DESCONTOS")
    print("=" * 50)

    print("\nMeses Mais Fortes por Categoria:")
    for category in sales_data['category_name'].unique():
        cat_data = category_season.loc[category]
        best_month = cat_data['total_sale'].idxmax()
        print(f"\n{category}:")
        print(f"Melhor mês: {best_month}")
        print(f"Vendas: R$ {cat_data.loc[best_month, 'total_sale']:,.2f}")
        print(f"Desconto médio: {cat_data.loc[best_month, 'discount'] * 100:.1f}%")

    print("\nImpacto dos Descontos por Categoria:")
    print(discount_impact)


def analyze_churn_risk(data):
    orders = data['orders'].copy()
    order_details = data['order_details'].copy()

    orders['order_date'] = pd.to_datetime(orders['order_date'])
    order_details['total_sale'] = order_details['unit_price'] * order_details['quantity'] * (
                1 - order_details['discount'])

    order_count = orders.groupby('customer_id')['order_id'].nunique()
    last_order_date = orders.groupby('customer_id')['order_date'].max()
    customer_sales = order_details.merge(orders, on='order_id').groupby('customer_id').agg({
        'total_sale': ['mean', 'sum'],
        'discount': 'mean'
    })

    last_order_overall = orders['order_date'].max()
    days_since_last = (last_order_overall - last_order_date).dt.days

    risk_levels = pd.cut(
        days_since_last,
        bins=[0, 30, 60, 90, float('inf')],
        labels=['Baixo', 'Médio', 'Alto', 'Crítico']
    )

    print("\n" + "=" * 50)
    print(" " * 15 + "ANÁLISE DE RISCO DE CHURN")
    print("=" * 50)

    print("\nDistribuição de Risco:")
    risk_dist = risk_levels.value_counts()
    for risk, count in risk_dist.items():
        print(f"{risk}: {count} clientes ({count / len(risk_levels) * 100:.1f}%)")
        print(f"- Média de pedidos: {order_count[risk_levels == risk].mean():.1f}")
        print(
            f"- Valor médio por pedido: R${customer_sales.loc[risk_levels == risk, ('total_sale', 'mean')].mean():.2f}")
        print(f"- Valor total: R${customer_sales.loc[risk_levels == risk, ('total_sale', 'sum')].sum():.2f}\n")

    return risk_levels

def main():
    data = load_all_data()
    #check_data_quality(data)
    analyze_sales_performance(data)
    analyze_active_vs_inactive_products(data)
    analyze_seasonality(data)
    analyze_geographic_distribution(data)
    analyze_cross_selling(data)
    analyze_customer_behavior(data)
    analyze_customer_patterns(data)
    analyze_temporal_patterns(data)
    analyze_category_seasonality(data)
    analyze_churn_risk(data)
    ticket_medio = analyze_ticket_medio(data['orders'], data['order_details'])
    churn_rate = analyze_churn(data['orders'])
    print(f'Ticket Médio: R${ticket_medio:.2f}')
    print(f'Taxa de Churn: {churn_rate:.2f}%')


if __name__ == "__main__":
    main()