# database.py
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
from contextlib import contextmanager


class StockDatabase:
    def __init__(self, db_path: str = "stock_data.db"):
        self.db_path = db_path
        self.init_database()
        
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            yield conn
        except sqlite3.Error as e:
            logging.error(f"数据库连接错误: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_database(self):
        """初始化数据库表结构"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 股票基本信息表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_info (
                        code TEXT PRIMARY KEY,
                        name TEXT,
                        industry TEXT,
                        area TEXT,
                        market TEXT,
                        list_date TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # 股票价格数据表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_prices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT,
                        date TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        amount REAL,
                        UNIQUE(code, date)
                    )
                ''')

                # 板块概念表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_concepts (
                        code TEXT,
                        concept TEXT,
                        PRIMARY KEY (code, concept)
                    )
                ''')

                # 模型训练记录表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS model_training_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        model_name TEXT,
                        training_date TIMESTAMP,
                        metrics TEXT,
                        parameters TEXT,
                        performance_score REAL
                    )
                ''')

                # 用户反馈表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_feedback (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        feedback_type TEXT,
                        content TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        related_stock TEXT,
                        rating INTEGER
                    )
                ''')

                conn.commit()
                logging.info("数据库初始化完成")
        except sqlite3.Error as e:
            logging.error(f"数据库初始化失败: {e}")
            raise

    def save_stock_info(self, stock_data: pd.DataFrame):
        """保存股票基本信息"""
        try:
            with self.get_connection() as conn:
                # 先清空表，再插入数据，保持replace的行为
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_info")
                conn.commit()
                
                # 插入新数据
                stock_data.to_sql('stock_info', conn, if_exists='append', index=False)
                logging.info(f"已保存 {len(stock_data)} 条股票基本信息")
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"保存股票基本信息失败: {e}")
            raise

    def save_stock_prices(self, price_data: pd.DataFrame):
        """保存股票价格数据"""
        try:
            with self.get_connection() as conn:
                # 处理重复数据的问题
                conn.execute("PRAGMA foreign_keys = ON")
                
                # 使用executemany批量插入，提高效率并处理重复键
                if not price_data.empty:
                    # 准备数据
                    data_tuples = [tuple(x) for x in price_data.to_numpy()]
                    columns = ', '.join(['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    placeholders = ', '.join(['?' for _ in range(len(price_data.columns))])
                    
                    # 使用INSERT OR REPLACE来处理重复数据
                    query = f"INSERT OR REPLACE INTO stock_prices ({columns}) VALUES ({placeholders})"
                    
                    cursor = conn.cursor()
                    cursor.executemany(query, data_tuples)
                    conn.commit()
                    logging.info(f"已保存 {len(price_data)} 条股票价格数据")
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"保存股票价格数据失败: {e}")
            raise

    def save_stock_concepts(self, concept_data: pd.DataFrame):
        """保存股票概念板块数据"""
        try:
            with self.get_connection() as conn:
                if not concept_data.empty:
                    # 使用INSERT OR REPLACE来处理重复数据
                    concept_data.to_sql('stock_concepts', conn, if_exists='append', index=False, method='multi')
                    logging.info(f"已保存 {len(concept_data)} 条股票概念数据")
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"保存股票概念数据失败: {e}")
            raise

    def get_stock_info(self, code: Optional[str] = None) -> pd.DataFrame:
        """获取股票基本信息"""
        try:
            with self.get_connection() as conn:
                if code:
                    query = "SELECT * FROM stock_info WHERE code = ?"
                    df = pd.read_sql_query(query, conn, params=(code,))
                else:
                    query = "SELECT * FROM stock_info"
                    df = pd.read_sql_query(query, conn)
                return df
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"获取股票基本信息失败: {e}")
            return pd.DataFrame()

    def get_stock_prices(self, code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取股票价格数据"""
        try:
            with self.get_connection() as conn:
                query = "SELECT * FROM stock_prices WHERE code = ?"
                params = [code]

                if start_date:
                    query += " AND date >= ?"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= ?"
                    params.append(end_date)

                query += " ORDER BY date"
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"获取股票价格数据失败: {e}")
            return pd.DataFrame()

    def get_stock_concepts(self, code: str = None) -> pd.DataFrame:
        """获取股票概念板块数据"""
        try:
            with self.get_connection() as conn:
                if code:
                    query = "SELECT * FROM stock_concepts WHERE code = ?"
                    df = pd.read_sql_query(query, conn, params=(code,))
                else:
                    query = "SELECT * FROM stock_concepts"
                    df = pd.read_sql_query(query, conn)
                return df
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logging.error(f"获取股票概念数据失败: {e}")
            return pd.DataFrame()

    def save_training_record(self, model_name: str, metrics: Dict, parameters: Dict, score: float):
        """保存模型训练记录"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO model_training_records (model_name, training_date, metrics, parameters, performance_score)
                    VALUES (?, ?, ?, ?, ?)
                ''', (model_name, datetime.now(), str(metrics), str(parameters), score))
                conn.commit()
                logging.info(f"已保存模型训练记录: {model_name}")
        except sqlite3.Error as e:
            logging.error(f"保存模型训练记录失败: {e}")
            raise

    def save_user_feedback(self, feedback_type: str, content: str, related_stock: str = None, rating: int = None):
        """保存用户反馈"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO user_feedback (feedback_type, content, related_stock, rating)
                    VALUES (?, ?, ?, ?)
                ''', (feedback_type, content, related_stock, rating))
                conn.commit()
                logging.info(f"已保存用户反馈: {feedback_type}")
        except sqlite3.Error as e:
            logging.error(f"保存用户反馈失败: {e}")
            raise