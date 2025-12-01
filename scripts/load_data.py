import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
import time

DB_USER = os.getenv('DB_USER', 'chistyakovartem214')
DB_PASS = os.getenv('DB_PASS', 'passartem')
DB_NAME = os.getenv('DB_NAME', 'pet_shop_dw')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def wait_for_db():
    max_retries = 30
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            print('БД развернуиа')
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f'Ожидание БД... ({i+1}/{max_retries})')
                time.sleep(2)
            else:
                print(f'Ошибка подключения к БД: {e}')
                raise
    return False

def create_staging_table():
    print('\nСоздание начальной таблицы...')
    
    create_table_sql = """
    DROP TABLE IF EXISTS staging_mock_data CASCADE;
    
    CREATE TABLE staging_mock_data (
        id INTEGER,
        customer_first_name VARCHAR(100),
        customer_last_name VARCHAR(100),
        customer_age INTEGER,
        customer_email VARCHAR(200),
        customer_country VARCHAR(100),
        customer_postal_code VARCHAR(50),
        customer_pet_type VARCHAR(50),
        customer_pet_name VARCHAR(100),
        customer_pet_breed VARCHAR(100),
        seller_first_name VARCHAR(100),
        seller_last_name VARCHAR(100),
        seller_email VARCHAR(200),
        seller_country VARCHAR(100),
        seller_postal_code VARCHAR(50),
        product_name VARCHAR(200),
        product_category VARCHAR(100),
        product_price NUMERIC(10,2),
        product_quantity INTEGER,
        sale_date VARCHAR(50),
        sale_customer_id INTEGER,
        sale_seller_id INTEGER,
        sale_product_id INTEGER,
        sale_quantity INTEGER,
        sale_total_price NUMERIC(10,2),
        store_name VARCHAR(200),
        store_location VARCHAR(200),
        store_city VARCHAR(100),
        store_state VARCHAR(100),
        store_country VARCHAR(100),
        store_phone VARCHAR(50),
        store_email VARCHAR(200),
        pet_category VARCHAR(100),
        product_weight NUMERIC(10,2),
        product_color VARCHAR(50),
        product_size VARCHAR(50),
        product_brand VARCHAR(100),
        product_material VARCHAR(100),
        product_description TEXT,
        product_rating NUMERIC(3,1),
        product_reviews INTEGER,
        product_release_date VARCHAR(50),
        product_expiry_date VARCHAR(50),
        supplier_name VARCHAR(200),
        supplier_contact VARCHAR(200),
        supplier_email VARCHAR(200),
        supplier_phone VARCHAR(50),
        supplier_address VARCHAR(200),
        supplier_city VARCHAR(100),
        supplier_country VARCHAR(100)
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    print('Начальная таблица создана')

def load_csv_files():
    print('\nПоиск CSV...')
    
    csv_files = sorted(glob.glob('/data/MOCK_DATA*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(' моки не найдены в папке /data/')
    
    print(f'Найдено: {len(csv_files)}')
    
    dfs = []
    
    for idx, file_path in enumerate(csv_files):
        print(f'  Загрузка {os.path.basename(file_path)}...', end=' ')
        
        try:
            df = pd.read_csv(file_path)
            
            filename = os.path.basename(file_path)
            if '(' in filename and ')' in filename:
                file_num = int(filename.split('(')[1].split(')')[0])
            else:
                file_num = 0
            
            id_columns = ['id', 'sale_customer_id', 'sale_seller_id', 'sale_product_id']
            for col in id_columns:
                if col in df.columns:
                    df[col] = df[col] + file_num * 1000
            
            dfs.append(df)
            print(f'({len(df)} строк)')
            
        except Exception as e:
            print(f'Ошибка: {e}')
            raise
    
    print('\nОбъединение...')
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f'Всего: {len(combined_df)}')
    
    print('\nЗагрузка в БД...')
    combined_df.to_sql(
        'staging_mock_data',
        engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    print('Данные загружены')
    
    return len(combined_df)

def verify_data():
    print('\nПроверка данных...')
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM staging_mock_data'))
        count = result.scalar()
        print(f'Записей в staging_mock_data: {count}')
        
        if count == 0:
            raise ValueError('Таблица пустая')
        
        return count

def main():
    try:
        print('=' * 60)
        print('Загрузка CSV в БД')
        print('=' * 60)
        
        wait_for_db()
        
        create_staging_table()
        
        total_rows = load_csv_files()
        
        verify_data()
        
        print('\n' + '=' * 60)
        print(f'Загружено {total_rows}')
        print('=' * 60)
        
    except Exception as e:
        print('\n' + '=' * 60)
        print(f'Ошибка: {e}')
        print('=' * 60)
        raise

if __name__ == '__main__':
    main()