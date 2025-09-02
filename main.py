#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据分析系统 - 主程序
从akshare下载股票数据并保存到本地SQLite数据库
"""

import logging
import argparse
import os
import time
from database import StockDatabase
from data_fetcher import DataFetcher
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='股票数据下载和更新工具')
    parser.add_argument('--db-path', type=str, default='stock_data.db',
                        help='SQLite数据库文件路径')
    parser.add_argument('--stock-limit', type=int, default=10,
                        help='要更新的股票数量限制，默认为10只')
    parser.add_argument('--price-period', type=str, default='1year',
                        choices=['1year', '3year', '5year', 'all'],
                        help='价格数据的时间周期')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='网络请求失败后的最大重试次数')
    parser.add_argument('--retry-delay', type=int, default=5,
                        help='重试之间的延迟时间（秒）')
    parser.add_argument('--debug', action='store_true',
                        help='启用调试日志模式')
    return parser.parse_args()


def setup_environment(args):
    """设置运行环境"""
    # 如果启用了调试模式，设置日志级别为DEBUG
    if args.debug:
        for handler in logging.root.handlers:
            handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.debug("已启用调试模式")
    
    # 确保数据目录存在
    db_dir = os.path.dirname(args.db_path) if os.path.dirname(args.db_path) else '.'
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logger.info(f"创建数据库目录: {db_dir}")
    
    return args


def main():
    """主函数，执行数据下载和保存操作"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 设置运行环境
        setup_environment(args)
        
        logger.info("=== 股票数据下载系统启动 ===")
        
        # 创建数据库实例
        db = StockDatabase(args.db_path)
        
        # 创建数据获取实例
        fetcher = DataFetcher(db, max_retries=args.max_retries, retry_delay=args.retry_delay)
        
        # 下载股票列表
        logger.info("开始下载股票列表...")
        stock_list = fetcher.fetch_stock_list()
        
        if not stock_list.empty:
            # 限制股票数量
            if args.stock_limit > 0:
                stock_list = stock_list.head(args.stock_limit)
                logger.info(f"限制处理 {args.stock_limit} 只股票")
            
            # 保存股票列表
            db.save_stock_info(stock_list)
            
            # 下载每只股票的价格数据和概念数据
            logger.info("开始下载股票价格数据...")
            
            # 创建进度条
            success_count = 0
            fail_count = 0
            
            for index, row in tqdm(stock_list.iterrows(), total=len(stock_list), desc="处理股票数据"):
                code = row['code']
                name = row['name'] if 'name' in row else "未知"
                
                try:
                    logger.debug(f"处理股票: {code} - {name}")
                    
                    # 下载价格数据
                    price_data = fetcher.fetch_stock_prices(code, period=args.price_period)
                    if not price_data.empty:
                        db.save_stock_prices(price_data)
                    
                    # 下载概念数据
                    concept_data = fetcher.fetch_stock_concepts(code)
                    if not concept_data.empty:
                        db.save_stock_concepts(concept_data)
                    
                    success_count += 1
                    
                    # 避免请求过于频繁
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"处理股票 {code} - {name} 失败: {e}")
                    fail_count += 1
                    continue
            
            logger.info(f"数据下载完成 - 成功: {success_count}, 失败: {fail_count}")
            logger.info("=== 股票数据下载系统运行完毕 ===")
        else:
            logger.warning("未获取到股票列表，无法继续下载数据")
            
    except Exception as e:
        logger.error(f"系统运行出错: {e}")
        raise


if __name__ == "__main__":
    main()