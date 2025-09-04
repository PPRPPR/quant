import logging
import time
import schedule
import argparse
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from database import StockDatabase
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('auto_updater')

class StockDataUpdater:
    """股票数据自动更新器"""
    
    def __init__(self):
        # 初始化数据库和数据获取器
        self.db = StockDatabase()
        self.fetcher = DataFetcher(self.db)
    
    def download_all_stocks_data(self, price_period: str = "all", incremental: bool = False):
        """下载所有股票的数据
        
        Args:
            price_period: 价格数据的时间周期，默认为"all"（所有历史数据）
            incremental: 是否使用增量更新模式，默认为False
        """
        logger.info(f"开始下载所有股票的数据（增量模式: {incremental}）...")
        start_time = time.time()
        
        try:
            if not incremental:
                # 非增量模式，调用原有的update_all_data方法
                self.fetcher.update_all_data(stock_limit=None, price_period=price_period)
            else:
                # 增量模式，先获取股票列表
                stock_list = self.fetcher.fetch_stock_list()
                if stock_list.empty:
                    logger.warning("未获取到股票列表，无法继续更新")
                    return False
                
                # 保存股票基本信息
                self.db.save_stock_info(stock_list)
                logger.info(f"已更新 {len(stock_list)} 只股票基本信息")
                
                # 获取所有股票的最新日期
                latest_dates_df = self.db.get_all_stocks_latest_dates()
                
                # 创建股票代码到最新日期的映射
                latest_dates = {row['code']: row['latest_date'] for _, row in latest_dates_df.iterrows()}
                
                total_stocks = len(stock_list)
                success_count = 0
                failed_count = 0
                
                logger.info(f"准备更新 {total_stocks} 只股票的详细数据（增量模式）")
                
                for i, (_, row) in enumerate(stock_list.iterrows()):
                    code = row['code']
                    name = row.get('name', '未知')
                    
                    # 显示进度
                    progress = (i + 1) / total_stocks * 100
                    logger.info(f"处理进度: {progress:.1f}% - 正在处理股票 {code}({name}) ({i+1}/{total_stocks})")
                    
                    try:
                        # 确定要获取的日期范围
                        if code in latest_dates and latest_dates[code]:
                            # 有历史数据，只获取最新日期之后的数据
                            latest_date = latest_dates[code]
                            # 最新日期加一天，确保不重复下载
                            latest_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
                            next_day_obj = latest_date_obj + timedelta(days=1)
                            next_day_str = next_day_obj.strftime('%Y%m%d')
                            today_str = datetime.now().strftime('%Y%m%d')
                            
                            if next_day_str <= today_str:
                                # 构建自定义日期范围
                                custom_period = f"{next_day_str}_{today_str}"
                                logger.info(f"股票 {code} 已有历史数据，获取 {latest_date} 之后的新数据")
                                
                                # 更新价格数据
                                try:
                                    prices = self.fetcher.fetch_stock_prices(code, period=custom_period)
                                    if not prices.empty:
                                        self.db.save_stock_prices(prices)
                                        logger.info(f"成功获取并保存股票 {code} 的新数据，共 {len(prices)} 条")
                                except Exception as e:
                                    logger.error(f"更新股票 {code} 价格数据失败: {e}")
                        else:
                            # 无历史数据，获取全部数据
                            logger.info(f"股票 {code} 无历史数据，获取全部数据")
                            try:
                                prices = self.fetcher.fetch_stock_prices(code, period=price_period)
                                if not prices.empty:
                                    self.db.save_stock_prices(prices)
                            except Exception as e:
                                logger.error(f"更新股票 {code} 价格数据失败: {e}")
                        
                        # 无论是否增量更新，都更新概念数据
                        try:
                            concepts = self.fetcher.fetch_stock_concepts(code)
                            if not concepts.empty:
                                self.db.save_stock_concepts(concepts)
                        except Exception as e:
                            logger.error(f"更新股票 {code} 概念数据失败: {e}")
                        
                        success_count += 1
                        
                        # 每处理5只股票暂停一下，避免请求过于频繁
                        if (i + 1) % 5 == 0 and i + 1 < total_stocks:
                            logger.info("处理频率控制，暂停3秒...")
                            time.sleep(3)
                        
                    except Exception as e:
                        logger.error(f"处理股票 {code} 时发生错误: {e}")
                        failed_count += 1
                        continue
                    
                logger.info(f"股票详细数据更新完成 - 成功: {success_count}, 失败: {failed_count}")
            
            end_time = time.time()
            logger.info(f"所有股票数据下载完成，总耗时: {end_time - start_time:.2f}秒")
            return True
        except Exception as e:
            logger.error(f"下载所有股票数据时发生错误: {e}")
            return False
    
    def update_daily_data(self):
        """更新每日股票数据"""
        logger.info("开始执行每日股票数据更新...")
        start_time = time.time()
        
        try:
            # 获取当天日期
            today = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"更新日期: {today}")
            
            # 调用download_all_stocks_data方法并启用增量更新模式
            # 这样可以确保只下载最新的数据，节省网络资源
            result = self.download_all_stocks_data(price_period="all", incremental=True)
            
            end_time = time.time()
            logger.info(f"每日股票数据更新完成，总耗时: {end_time - start_time:.2f}秒")
            return result
        except Exception as e:
            logger.error(f"更新每日股票数据时发生错误: {e}")
            return False
    
    def start_scheduled_updates(self, update_time: str = "09:00"):
        """启动定时自动更新
        
        Args:
            update_time: 每日更新时间，格式为"HH:MM"，默认为"09:00"
        """
        logger.info(f"设置每日自动更新，更新时间: {update_time}")
        
        # 安排每日定时任务
        schedule.every().day.at(update_time).do(self.update_daily_data)
        
        logger.info("自动更新服务已启动，按Ctrl+C停止...")
        
        try:
            # 无限循环，等待任务执行
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("自动更新服务已停止")

if __name__ == "__main__":
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='股票数据自动更新工具')
    
    # 添加子命令
    subparsers = parser.add_subparsers(dest='command', help='选择要执行的命令')
    
    # 下载所有股票数据的子命令
    download_parser = subparsers.add_parser('download-all', help='下载所有股票的数据')
    download_parser.add_argument('--period', type=str, default='all', 
                              help='价格数据的时间周期，可选值: all, 1year, 3year, 5year')
    download_parser.add_argument('--incremental', action='store_true', default=False, 
                              help='使用增量更新模式，只下载最新数据')
    
    # 执行每日更新的子命令
    daily_parser = subparsers.add_parser('daily-update', help='执行一次每日更新')
    
    # 启动定时更新服务的子命令
    schedule_parser = subparsers.add_parser('start-schedule', help='启动定时自动更新服务')
    schedule_parser.add_argument('--time', type=str, default='09:00', 
                             help='每日更新时间，格式为"HH:MM"')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 创建更新器实例
    updater = StockDataUpdater()
    
    # 根据命令执行相应的操作
    if args.command == 'download-all':
        updater.download_all_stocks_data(price_period=args.period, incremental=args.incremental)
    elif args.command == 'daily-update':
        updater.update_daily_data()
    elif args.command == 'start-schedule':
        updater.start_scheduled_updates(update_time=args.time)
    else:
        parser.print_help()