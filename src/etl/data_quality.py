import pandas as pd


def check_data_quality(data):
    print("\n" + "=" * 50)
    print(" " * 15 + "RELATÓRIO DE QUALIDADE")
    print("=" * 50 + "\n")

    for table_name, df in data.items():
        print(f" {table_name.upper()} ")
        print(f"Total de registros: {len(df):,}")

        missing = df.isnull().sum()
        missing = missing[missing > 0]

        if not missing.empty:
            print("\nValores ausentes:")
            for col, count in missing.items():
                print(f"  • {col:<20} {count:>5} ({(count / len(df) * 100):>6.1f}%)")

        print("-" * 50)

    return True