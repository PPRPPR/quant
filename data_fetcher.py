# data_fetcher.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import time
from retry import retry
from database import StockDatabase

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, db: StockDatabase, max_retries: int = 3, retry_delay: int = 5):
        self.db = db
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    @retry(Exception, tries=3, delay=2, backoff=2)
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        try:
            logger.info("开始获取A股股票列表...")
            
            # 获取A股股票列表
            stock_list = ak.stock_info_a_code_name()
            if stock_list.empty:
                logger.warning("获取的股票列表为空")
                return pd.DataFrame()
                
            stock_list.columns = ['code', 'name']
            logger.info(f"获取到 {len(stock_list)} 只股票基本信息")

            # 获取上海证券交易所股票信息
            try:
                stock_info_sh = ak.stock_info_sh_name_code()
                if not stock_info_sh.empty and len(stock_info_sh.columns) >= 4:
                    stock_info_sh = stock_info_sh.iloc[:, [0, 1, 2, 3]]
                    stock_info_sh.columns = ['code', 'name', 'industry', 'area']
                    stock_info_sh['market'] = 'SH'
                else:
                    stock_info_sh = pd.DataFrame()
            except Exception as e:
                logger.warning(f"获取上海证券交易所股票信息失败: {e}")
                stock_info_sh = pd.DataFrame()

            # 获取深圳证券交易所股票信息
            try:
                stock_info_sz = ak.stock_info_sz_name_code()
                if not stock_info_sz.empty and len(stock_info_sz.columns) >= 4:
                    stock_info_sz = stock_info_sz.iloc[:, [0, 1, 2, 3]]
                    stock_info_sz.columns = ['code', 'name', 'industry', 'area']
                    stock_info_sz['market'] = 'SZ'
                else:
                    stock_info_sz = pd.DataFrame()
            except Exception as e:
                logger.warning(f"获取深圳证券交易所股票信息失败: {e}")
                stock_info_sz = pd.DataFrame()

            # 合并上海和深圳的股票信息
            stock_info = pd.concat([stock_info_sh, stock_info_sz], ignore_index=True)
            
            # 合并股票列表和详细信息
            if not stock_info.empty:
                result = pd.merge(stock_list, stock_info, on='code', how='left')
                # 处理可能出现的name_x, name_y重复列名问题
                if 'name_x' in result.columns and 'name_y' in result.columns:
                    result['name'] = result['name_x'].fillna(result['name_y'])
                    result = result.drop(['name_x', 'name_y'], axis=1)
            else:
                result = stock_list.copy()
                result['industry'] = None
                result['area'] = None
                result['market'] = None
            
            # 获取上市日期 - 使用try-except并设置默认值
            try:
                # 尝试使用另一种方式获取上市日期
                stock_zh_a_stock_ipo = ak.stock_zh_a_stock_ipo(symbol=result['code'][0]) if not result.empty else pd.DataFrame()
                if not stock_zh_a_stock_ipo.empty:
                    # 这里简化处理，实际可能需要更复杂的逻辑来匹配上市日期
                    result['list_date'] = datetime.now().strftime('%Y-%m-%d')
                else:
                    result['list_date'] = datetime.now().strftime('%Y-%m-%d')
            except Exception as e:
                logger.warning(f"获取上市日期失败: {e}")
                result['list_date'] = datetime.now().strftime('%Y-%m-%d')

            logger.info("股票列表获取完成")
            return result
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise

    @retry(Exception, tries=3, delay=2, backoff=2)
    def fetch_stock_prices(self, code: str, period: str = "1year") -> pd.DataFrame:
        """获取股票价格数据"""
        try:
            logger.info(f"开始获取股票 {code} 的价格数据，周期: {period}")
            
            # 确定日期范围
            if period == "1year":
                end_date = datetime.now()
                start_date = end_date - timedelta(days=365)
            elif period == "3year":
                end_date = datetime.now()
                start_date = end_date - timedelta(days=1095)
            elif period == "5year":
                end_date = datetime.now()
                start_date = end_date - timedelta(days=1825)
            elif period == "all":
                # 获取所有历史数据
                start_date = "19900101"  # A股开市时间
                end_date = datetime.now()
            else:
                # 自定义日期范围
                start_date = period.split('_')[0] if '_' in period else "20200101"
                end_date = period.split('_')[1] if '_' in period else datetime.now().strftime('%Y%m%d')

            # 格式化日期字符串
            start_date_str = start_date if isinstance(start_date, str) else start_date.strftime('%Y%m%d')
            end_date_str = end_date if isinstance(end_date, str) else end_date.strftime('%Y%m%d')

            # 获取股票历史数据
            stock_zh_a_hist_df = ak.stock_zh_a_hist(
                symbol=code, 
                period="daily",
                start_date=start_date_str,
                end_date=end_date_str,
                adjust="qfq"  # 前复权
            )

            if not stock_zh_a_hist_df.empty:
                # 重命名列
                column_mapping = {
                    '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low',
                    '收盘': 'close', '成交量': 'volume', '成交额': 'amount',
                    '涨跌幅': 'pct_change', '换手率': 'turnover_rate'
                }
                
                # 只选择存在的列进行重命名
                existing_columns = {k: v for k, v in column_mapping.items() if k in stock_zh_a_hist_df.columns}
                stock_zh_a_hist_df = stock_zh_a_hist_df.rename(columns=existing_columns)

                stock_zh_a_hist_df['code'] = code
                
                # 确保需要的列存在
                required_columns = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
                available_columns = [col for col in required_columns if col in stock_zh_a_hist_df.columns]
                
                # 添加缺失的列并设置为None
                for col in required_columns:
                    if col not in stock_zh_a_hist_df.columns:
                        stock_zh_a_hist_df[col] = None
                
                stock_zh_a_hist_df = stock_zh_a_hist_df[available_columns + [col for col in ['amount', 'pct_change', 'turnover_rate'] if col in stock_zh_a_hist_df.columns]]
                
                logger.info(f"获取到股票 {code} 的 {len(stock_zh_a_hist_df)} 条价格数据")
            else:
                logger.warning(f"未获取到股票 {code} 的价格数据")

            return stock_zh_a_hist_df
        except Exception as e:
            logger.error(f"获取股票{code}价格数据失败: {e}")
            raise

    @retry(Exception, tries=3, delay=2, backoff=2)
    def fetch_stock_concepts(self, code: str) -> pd.DataFrame:
        """获取股票概念板块数据"""
        try:
            logger.info(f"开始获取股票 {code} 的概念板块数据")
            
            # 尝试多种方式获取股票概念
            concept_methods = [
                lambda: ak.stock_board_concept_name_ths(),
                lambda: ak.stock_board_concept_cons_ths(symbol=code),
                lambda: ak.stock_board_concept_name_ths()
            ]
            
            concepts = []
            concept_df = pd.DataFrame()
            
            for method in concept_methods:
                try:
                    temp_df = method()
                    if not temp_df.empty:
                        concept_df = temp_df
                        break
                except Exception as inner_e:
                    logger.debug(f"尝试一种概念获取方法失败: {inner_e}")
                    continue
            
            # 提取概念信息
            if not concept_df.empty:
                # 根据不同的返回格式提取概念
                if '概念名称' in concept_df.columns:
                    concepts = concept_df['概念名称'].tolist()
                elif 'concept_name' in concept_df.columns:
                    concepts = concept_df['concept_name'].tolist()
                elif '板块名称' in concept_df.columns:
                    concepts = concept_df['板块名称'].tolist()
                
                # 去重并限制数量
                concepts = list(set(concepts))[:10]  # 最多保留10个概念
                
                if concepts:
                    result = pd.DataFrame({
                        'code': [code] * len(concepts),
                        'concept': concepts
                    })
                    logger.info(f"获取到股票 {code} 的 {len(concepts)} 个概念")
                    return result
            
            logger.warning(f"未获取到股票 {code} 的概念数据")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取股票{code}概念数据失败: {e}")
            raise

    def update_all_data(self, stock_limit: Optional[int] = 10, price_period: str = "1year"):
        """更新所有数据"""
        logger.info("开始更新股票数据...")
        start_time = time.time()

        # 获取并保存股票列表
        try:
            stock_list = self.fetch_stock_list()
            if not stock_list.empty:
                self.db.save_stock_info(stock_list)
                logger.info(f"已更新 {len(stock_list)} 只股票基本信息")
            else:
                logger.warning("未获取到股票列表，无法继续更新")
                return
        except Exception as e:
            logger.error(f"更新股票列表失败: {e}")
            return

        # 更新部分或全部股票的价格和概念数据
        if not stock_list.empty:
            # 如果指定了股票数量限制，则只处理前stock_limit只股票
            stocks_to_process = stock_list.head(stock_limit) if stock_limit else stock_list
            total_stocks = len(stocks_to_process)
            
            logger.info(f"准备更新 {total_stocks} 只股票的详细数据")
            
            success_count = 0
            failed_count = 0
            
            for i, (_, row) in enumerate(stocks_to_process.iterrows()):
                code = row['code']
                name = row.get('name', '未知')
                
                # 显示进度
                progress = (i + 1) / total_stocks * 100
                logger.info(f"处理进度: {progress:.1f}% - 正在处理股票 {code}({name}) ({i+1}/{total_stocks})")
                
                try:
                    # 更新价格数据
                    try:
                        prices = self.fetch_stock_prices(code, period=price_period)
                        if not prices.empty:
                            self.db.save_stock_prices(prices)
                    except Exception as e:
                        logger.error(f"更新股票 {code} 价格数据失败: {e}")
                        
                    # 更新概念数据
                    try:
                        concepts = self.fetch_stock_concepts(code)
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
        logger.info(f"所有数据更新完成，总耗时: {end_time - start_time:.2f}秒")