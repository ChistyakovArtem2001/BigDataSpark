import os
from sqlalchemy import create_engine, text

DB_USER = os.getenv('DB_USER', 'chistyakovartem214')
DB_PASS = os.getenv('DB_PASS', 'passartem')
DB_NAME = os.getenv('DB_NAME', 'pet_shop_dw')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def run_dml():
    print('DML...')
    
    dml_path = '/sql/02_dml.sql'
    
    if not os.path.exists(dml_path):
        print(f'Файл не найден: {dml_path}')
        return False
    
    with open(dml_path, 'r', encoding='utf-8') as f:
        dml_sql = f.read()
    
    print('DML...')
    
    try:
        with engine.connect() as conn:
            conn.execute(text(dml_sql))
            conn.commit()
        
        print('DML выполнен успешно')
        
        print('\nПроверка:')
        with engine.connect() as conn:
            tables = [
                'staging_mock_data',
                'fact_sales',
                'dimension_customers',
                'dimension_sellers',
                'dimension_products',
                'dimension_stores'
            ]
            
            for table in tables:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {table}'))
                count = result.scalar()
                status = 'ok' if count > 0 else 'no'
                print(f'  {status} {table}: {count:,} записей')
        
        return True
        
    except Exception as e:
        print(f'Ошибка DML: {e}')
        raise

def main():
    try:
        print('=' * 60)
        print('Заполнение DML')
        print('=' * 60)
        
        run_dml()
        
        print('\n' + '=' * 60)
        print('Ok')
        print('=' * 60)
        
    except Exception as e:
        print('\n' + '=' * 60)
        print(f'Ошибка: {e}')
        print('=' * 60)
        raise

if __name__ == '__main__':
    main()
