import os
from sqlalchemy import create_engine, text

DB_USER = os.getenv('DB_USER', 'chistyakovartem214')
DB_PASS = os.getenv('DB_PASS', 'passartem')
DB_NAME = os.getenv('DB_NAME', 'pet_shop_dw')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def run_ddl():
    print('DDL...')
    
    ddl_path = '/sql/01_ddl.sql'
    
    if not os.path.exists(ddl_path):
        print(f'Файл не найден: {ddl_path}')
        return False
    
    with open(ddl_path, 'r', encoding='utf-8') as f:
        ddl_sql = f.read()
    
    print('DDL...')
    
    try:
        with engine.connect() as conn:
            conn.execute(text(ddl_sql))
            conn.commit()
        
        print('DDL выполнен успешно')
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name != 'staging_mock_data'
            """))
            table_count = result.scalar()
            print(f'Создано таблиц: {table_count}')
        
        return True
        
    except Exception as e:
        print(f'Ошибка DDL: {e}')
        raise

def main():
    try:
        print('=' * 60)
        print('Создание табл DLL')
        print('=' * 60)
        
        run_ddl()
        
        print('=' * 60)
        print('Ok')
        print('=' * 60)
        
    except Exception as e:
        print('=' * 60)
        print(f'Ошибка: {e}')
        print('=' * 60)
        raise

if __name__ == '__main__':
    main()