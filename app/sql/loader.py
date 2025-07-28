import os
from pathlib import Path
from loguru import logger
from typing import Dict, Optional


class SQLLoader:
    def __init__(self, sql_directory: str = "app/sql/queries"):
        self.sql_directory = Path(sql_directory)
        self.queries: Dict[str, str] = {}
        self.load_all_queries()
    
    def load_all_queries(self):
        \"\"\"Load all SQL files from the queries directory\"\"\"
        if not self.sql_directory.exists():
            logger.warning(f"SQL directory does not exist: {self.sql_directory}")
            return
        
        for sql_file in self.sql_directory.glob("*.sql"):
            query_name = sql_file.stem
            try:
                with open(sql_file, 'r', encoding='utf-8') as f:
                    self.queries[query_name] = f.read().strip()
                logger.info(f"Loaded SQL query: {query_name}")
            except Exception as e:
                logger.error(f"Failed to load SQL file {sql_file}: {str(e)}")
    
    def get_query(self, query_name: str) -> Optional[str]:
        \"\"\"Get a specific query by name\"\"\"
        return self.queries.get(query_name)
    
    def list_available_queries(self) -> list:
        \"\"\"List all available query names\"\"\"
        return list(self.queries.keys())
    
    def reload_queries(self):
        \"\"\"Reload all queries from files\"\"\"
        self.queries.clear()
        self.load_all_queries()

# Global SQL loader instance
sql_loader = SQLLoader()
