"""
NATURAL LANGUAGE TO SQL POC WORKFLOW:
1. Database Setup: Creates in-memory SQLite DB with sample data
2. Query Processing: Converts English questions to SQL via OpenAI
3. Execution: Runs SQL with error handling and retries
4. Results: Displays formatted output
"""

import os
import sqlite3
from openai import OpenAI
from typing import Tuple, Optional, List, Dict, Any
from dotenv import load_dotenv


load_dotenv()


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
if not client.api_key:
    raise ValueError("OPENAI_API_KEY not found in environment variables")

class DatabaseManager:
    """Handles database setup and operations"""
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database with sample schema and data"""
        cursor = self.conn.cursor()
        
        
        create_products = """
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            city TEXT,
            sale_date DATE,
            quantity INTEGER,
            price DECIMAL(10,2)
        )
        """
        
        create_users = """
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT,
            email TEXT,
            registration_date DATE
        )
        """
        
        create_purchases = """
        CREATE TABLE purchases (
            purchase_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            purchase_date DATE,
            quantity INTEGER,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        )
        """
        
     
        cursor.execute(create_products)
        cursor.execute(create_users)
        cursor.execute(create_purchases)
        

        products = [
            (1, 'Laptop', 'Bangalore', '2023-10-01', 5, 999.99),
            (2, 'Smartphone', 'Bangalore', '2023-10-02', 10, 699.99),
            (3, 'Tablet', 'Mumbai', '2023-10-03', 8, 499.99),
            (4, 'Headphones', 'Bangalore', '2023-10-04', 15, 149.99),
            (5, 'Monitor', 'Delhi', '2023-10-05', 3, 249.99),
        ]
        
        users = [
            (1, 'John Doe', 'john@example.com', '2023-01-15'),
            (2, 'Jane Smith', 'jane@example.com', '2023-02-20'),
            (3, 'Bob Johnson', 'bob@example.com', '2023-03-10'),
        ]
        
        purchases = [
            (1, 1, 1, '2023-10-01', 1),
            (2, 1, 2, '2023-10-02', 2),
            (3, 2, 1, '2023-10-03', 1),
            (4, 2, 3, '2023-10-04', 1),
            (5, 2, 4, '2023-10-05', 3),
            (6, 3, 2, '2023-10-06', 1),
            (7, 3, 5, '2023-10-07', 2),
        ]
        
        cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)", products)
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", users)
        cursor.executemany("INSERT INTO purchases VALUES (?, ?, ?, ?, ?)", purchases)
        self.conn.commit()

    def get_schema(self) -> List[Dict[str, Any]]:
        """Retrieve database schema information"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        schema = []
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            schema.append({
                'table': table_name,
                'columns': [col[1] for col in columns],
                'types': [col[2] for col in columns]
            })
        return schema
    
    def execute_sql(self, sql: str) -> Tuple[bool, Optional[Tuple[List[str], List[Any]]], Optional[str]]:
        """Execute SQL query and return results"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [description[0] for description in cursor.description] if cursor.description else []
            return True, (columns, results), None
        except Exception as e:
            return False, None, str(e)

class SQLGenerator:
   
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def generate_sql(self, question: str, max_retries: int = 3) -> Dict[str, Any]:
        """Generate and execute SQL with error correction"""
        retry_count = 0
        last_error = None
        
        while retry_count <= max_retries:
            try:
                sql = self._generate_with_openai(question, retry_count > 0, last_error)
                print(f"Attempt {retry_count + 1}: Generated SQL:\n{sql}\n")
                success, results, error = self.db.execute_sql(sql)
                
                if success:
                    return {
                        'success': True,
                        'sql': sql,
                        'results': results,
                        'attempts': retry_count + 1
                    }
                else:
                    last_error = error
                    print(f"Execution error: {error}\n")
                    retry_count += 1
            except Exception as e:
                last_error = str(e)
                print(f"Generation error: {last_error}\n")
                retry_count += 1
        
        return {
            'success': False,
            'error': last_error,
            'attempts': retry_count
        }
    
    def _generate_with_openai(self, question: str, is_retry: bool, last_error: Optional[str]) -> str:
        """Generate SQL using OpenAI API"""
        schema = self.db.get_schema()
        schema_info = "\n".join([
            f"Table {table['table']} columns: {', '.join(table['columns'])}"
            for table in schema
        ])
        
        if is_retry and last_error:
            prompt = f"""
            The previous SQL query failed with error: {last_error}
            Please correct the SQL query for this question: {question}
            Database schema:
            {schema_info}
            
            Return ONLY the SQL query with no additional explanation or formatting.
            """
        else:
            prompt = f"""
            Convert this natural language question into a SQL query for SQLite:
            Question: {question}
            Database schema:
            {schema_info}
            
            Return ONLY the SQL query with no additional explanation or formatting.
            """
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL expert that converts questions to accurate SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=200
        )
        
        sql = response.choices[0].message.content.strip()
        
     
        for prefix in ["```sql", "```"]:
            if sql.startswith(prefix):
                sql = sql[len(prefix):].split("```")[0].strip()
        return sql

def display_results(result: Dict[str, Any]):
  
    print(f"\n{'='*50}")
    if result['success']:
        print(f"SUCCESS after {result['attempts']} attempts")
        print("Generated SQL:")
        print(result['sql'])
        
        columns, data = result['results']
        print("\nResults:")
        print("\t".join(columns))
        for row in data:
            print("\t".join(str(item) for item in row))
    else:
        print("FAILED after maximum retries")
        print(f"Last error: {result['error']}")

def main():
 
    try:
        db = DatabaseManager()
        generator = SQLGenerator(db)
        
        # test questions 
        questions = [
            "List users who spent more than 1000 in Bangalore",
            
        ]
        
        for question in questions:
            print(f"\nProcessing question: {question}")
            result = generator.generate_sql(question)
            display_results(result)
    
    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        if 'db' in locals():
            db.conn.close()

if __name__ == "__main__":
    main()
