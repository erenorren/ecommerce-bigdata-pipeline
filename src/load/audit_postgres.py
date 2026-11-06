import pandas as pd
from sqlalchemy import create_engine

def main():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/ecommerce_dw')
    
    query = """
    SELECT 
        COUNT(*) as total_baris,
        COUNT(DISTINCT order_id) as order_id_unik,
        SUM(is_batal) as total_batal
    FROM fact_orders;
    """
    
    df = pd.read_sql(query, engine)
    
    print("        AUDIT DATABASE POSTGRESQL (TAHAP 4)       ")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()