import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from typing import Callable, Dict, Any
import pandas_ta as ta
import numpy as np
import xlsxwriter

# 忽略 pandas 的 SettingWithCopyWarning
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)


# ==============================================================================
# 工具函数
# ==============================================================================
def format_stock_code(code: str) -> str:
    """根据股票代码的开头数字，添加SH或SZ前缀。"""
    code_str = str(code).zfill(6)
    if code_str.startswith('6'):
        return 'sh' + code_str
    elif code_str.startswith(('0', '3')):
        return 'sz' + code_str
    elif code_str.startswith(('4', '8')):
        return 'bj' + code_str
    return code_str


# ==============================================================================
# 配置类
# ==============================================================================
class Config:
    """
    程序配置类，用于管理路径、重试次数等全局设置。
    """

    def __init__(self):
        self.HOME_DIRECTORY = os.path.expanduser('~')
        self.SAVE_DIRECTORY = os.path.join(self.HOME_DIRECTORY, 'Downloads', 'CoreNews_Reports')
        self.TEMP_DATA_DIRECTORY = os.path.join(self.SAVE_DIRECTORY, 'ShareData')
        self.DATA_FETCH_RETRIES = 5
        self.DATA_FETCH_DELAY = 10
        self.MAX_WORKERS = 16


# ==============================================================================
# 数据获取类
# ==============================================================================
class DataFetcher:
    """
    负责从 Akshare 获取数据，并实现缓存和并行下载功能。
    """

    def __init__(self, config: Config):
        self.config = config
        self.today_str = datetime.now().strftime("%Y%m%d")
        self.executor = ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS)
        os.makedirs(self.config.TEMP_DATA_DIRECTORY, exist_ok=True)
        self.macd_cache_file = os.path.join(self.config.TEMP_DATA_DIRECTORY, 'MACD_hist_data_cache.txt')

    def get_file_path(self, base_name: str, is_cleaned: bool = False) -> str:
        """根据基础文件名和当前日期生成完整的文件路径。如果 is_cleaned 为 True，则添加 '_经清洗' 后缀。"""
        suffix = "_经清洗" if is_cleaned else ""
        file_name = f"{base_name}{suffix}_{self.today_str}.txt"
        return os.path.join(self.config.TEMP_DATA_DIRECTORY, file_name)

    def load_data_from_txt(self, file_path: str) -> pd.DataFrame:
        """从 | 分隔的 TXT 文件加载数据。"""
        if os.path.exists(file_path):
            try:
                # 尝试加载数据
                df = pd.read_csv(file_path, sep='|', encoding='utf-8', dtype={'股票代码': str})
                return df
            except Exception as e:
                print(f"[WARN] 错误：加载临时文件 {os.path.basename(file_path)} 失败: {e}，将重新获取。")
        return pd.DataFrame()

    def save_data_to_txt(self, df: pd.DataFrame, file_path: str):
        """将 DataFrame 保存到 | 分隔的 TXT 文件。"""
        try:
            df.to_csv(file_path, sep='|', index=False, encoding='utf-8')
            print(f"数据已保存到临时文件: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"[ERROR] 错误：保存数据到临时文件 {os.path.basename(file_path)} 失败: {e}")

    def fetch_with_cache(self, fetch_func: Callable, file_base_name: str, **kwargs: Any) -> pd.DataFrame:
        """带缓存和重试功能的数据获取函数，优先检查已清洗缓存。"""

        # 1. 优先检查是否存在已清洗的缓存文件
        cleaned_file_path = self.get_file_path(file_base_name, is_cleaned=True)
        cached_cleaned_df = self.load_data_from_txt(cleaned_file_path)
        if not cached_cleaned_df.empty:
            print(f"发现已清洗缓存文件: {os.path.basename(cleaned_file_path)}，直接加载数据。")
            return cached_cleaned_df

        # 2. 检查是否存在未清洗的缓存文件
        raw_file_path = self.get_file_path(file_base_name, is_cleaned=False)
        cached_raw_df = self.load_data_from_txt(raw_file_path)
        if not cached_raw_df.empty:
            print(f"发现原始临时文件: {os.path.basename(raw_file_path)}，直接加载数据。")
            return cached_raw_df  # 返回未清洗的，让 processor 重新清洗并保存/删除

        # 3. 如果都没有，则进行 API 抓取
        for i in range(self.config.DATA_FETCH_RETRIES):
            try:
                print(f"正在尝试第 {i + 1}/{self.config.DATA_FETCH_RETRIES} 次获取数据: {file_base_name}...")
                df = fetch_func(**kwargs)
                if df is not None and not df.empty:
                    print("数据获取成功。")
                    self.save_data_to_txt(df, raw_file_path)  # 保存原始数据
                    return df
                else:
                    print("[WARN] 数据返回为空或无效，将重试。")
                    time.sleep(self.config.DATA_FETCH_DELAY)
            except Exception as e:
                print(f"[ERROR] 获取数据时出错: {e}，将在 {self.config.DATA_FETCH_DELAY} 秒后重试。")
                time.sleep(self.config.DATA_FETCH_DELAY)
        print(f"[ERROR] 所有重试均失败，将返回空 DataFrame: {file_base_name}")
        return pd.DataFrame()

    def get_top_industry_stocks(self) -> pd.DataFrame:
        """
        获取涨跌幅前五的板块及其成分股，并进行规范化处理。
        """
        print("\n>>> 正在获取东方财富-沪深京板块-行业板块数据...")
        # 1. 获取行业板块名称列表
        # 注意：这里获取的行业板块名称列表不涉及清洗（ST过滤等），因此不应该调用 clean_data
        all_industries_df = self.fetch_with_cache(ak.stock_board_industry_name_em, '行业板块名称')
        if all_industries_df.empty:
            print("警告：未能获取行业板块名称列表，无法获取成分股。")
            return pd.DataFrame()
        top_industries = all_industries_df.sort_values(by='涨跌幅', ascending=False).head(10)
        if top_industries.empty:
            print("警告：未能找到涨幅前的板块。")
            return pd.DataFrame()
        print(f"  - 涨跌幅前排板块是: {top_industries['板块名称'].tolist()}")
        all_constituents = []
        with ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            future_to_industry = {
                executor.submit(
                    self.fetch_with_cache,
                    ak.stock_board_industry_cons_em,
                    f"板块成分股_{row['板块名称']}",
                    symbol=row['板块名称']
                ): row['板块名称']
                for _, row in top_industries.iterrows()
            }
            for future in as_completed(future_to_industry):
                industry_name = future_to_industry[future]
                try:
                    constituents_df = future.result()
                    if not constituents_df.empty:
                        print(f"  - 成功获取板块 '{industry_name}' 的成分股。")
                        # 添加板块名称列
                        constituents_df['所属板块'] = industry_name
                        # 确保股票代码列为字符串格式，防止0丢失
                        if '代码' in constituents_df.columns:
                            constituents_df.rename(columns={'代码': '股票代码'}, inplace=True)
                        constituents_df['股票代码'] = constituents_df['股票代码'].astype(str).str.zfill(6)
                        constituents_df['完整股票编码'] = constituents_df['股票代码'].apply(format_stock_code)
                        all_constituents.append(constituents_df)
                    else:
                        print(f"  - 警告：未能获取板块 '{industry_name}' 的成分股数据。")
                except Exception as e:
                    print(f"  - [ERROR] 获取板块 '{industry_name}' 成分股时出错: {e}")

        if all_constituents:
            merged_df = pd.concat(all_constituents, ignore_index=True)
            print(f"  - 已合并所有前板块成分股数据，共 {len(merged_df)} 条。")
            return merged_df
        else:
            print("  - 所有板块成分股数据均获取失败。")
            return pd.DataFrame()

    def fetch_hist_data_parallel(self, codes: list, days: int) -> pd.DataFrame:
        """并行获取指定股票代码的历史数据，并缓存到本地文件。"""
        print(f"\n正在为 {len(codes)} 只股票下载 {days} 天的历史数据，使用15个线程并行处理。")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        if os.path.exists(self.macd_cache_file):
            # 检查缓存是否过期（例如，如果缓存不是今天创建的，则重新下载）
            cache_date = datetime.fromtimestamp(os.path.getmtime(self.macd_cache_file)).strftime("%Y%m%d")
            if cache_date == self.today_str:
                print(f"发现今日历史数据缓存文件，直接加载。")
                return pd.read_csv(self.macd_cache_file, sep='|', encoding='utf-8', dtype={'股票代码': str})
            else:
                print("发现旧的历史数据缓存文件，将重新下载。")

        all_data = []
        # 将代码转换为完整的市场编码
        future_to_code = {}
        with ThreadPoolExecutor(max_workers=15) as executor:
            for code in codes:
                future = executor.submit(
                    ak.stock_zh_a_hist_tx,
                    symbol=format_stock_code(code),
                    start_date=start_date_str,
                    end_date=end_date_str,
                    adjust="hfq"
                )
                future_to_code[future] = code

            for i, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    hist_df = future.result()
                    if hist_df is not None and not hist_df.empty:
                        hist_df['股票代码'] = code
                        # 确保日期是字符串格式，避免excel出错
                        if '日期' in hist_df.columns:
                            hist_df['日期'] = pd.to_datetime(hist_df['日期']).dt.strftime('%Y-%m-%d')
                        all_data.append(hist_df)

                except Exception as e:
                    print(f"[ERROR] 错误：获取 {code} 的历史数据时出错: {e}，已跳过。")
        if all_data:
            merged_df = pd.concat(all_data, ignore_index=True)
            self.save_data_to_txt(merged_df, self.macd_cache_file)
            return merged_df
        print("[WARN] 未能成功下载任何股票的历史数据。")
        return pd.DataFrame()


# ==============================================================================
# 数据处理类
# ==============================================================================
class DataProcessor:
    """
    负责对获取的数据进行清洗、合并和技术指标计算。
    """

    def __init__(self, data_fetcher: DataFetcher):
        self.fetcher = data_fetcher
        self.executor = ThreadPoolExecutor(max_workers=self.fetcher.config.MAX_WORKERS)
        self.start_date_for_ta = (datetime.now() - pd.DateOffset(months=6)).strftime("%Y%m%d")
        self.end_date_for_ta = datetime.now().strftime("%Y%m%d")
        self.code_aliases = {'代码': '股票代码', '股票代码': '股票代码', '证券代码': '股票代码'}
        self.name_aliases = {'名称': '股票简称', '股票名称': '股票简称', '股票简称': '股票简称'}
        self.price_aliases = {'最新价': '最新价', '现价': '最新价'}

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 的列名，确保'股票代码'和'股票简称'等列存在。"""
        if df.empty:
            return df
        found_code_col = False
        for old_name, new_name in self.code_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_code_col = True
                break
        if not found_code_col:
            # print(f"[WARN] 未能在数据中找到股票代码列，原始列名: {df.columns.tolist()}")
            return pd.DataFrame()
        found_name_col = False
        for old_name, new_name in self.name_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_name_col = True
                break
        if not found_name_col and '股票简称' not in df.columns:
            # print(f"[WARN] 未能在数据中找到股票名称列，原始列名: {df.columns.tolist()}")
            pass  # 允许没有股票简称，但可能有股票代码
        found_price_col = False
        for old_name, new_name in self.price_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_price_col = True
                break
        if not found_price_col and '最新价' not in df.columns:
            # print(f"[WARN] 未能在数据中找到价格列，原始列名: {df.columns.tolist()}")
            pass
        return df

    def clean_data(self, df: pd.DataFrame, df_name: str) -> pd.DataFrame:
        """通用数据清洗函数，处理缺失值、重复值并去除ST股，并保存清洗后的数据，删除原始文件。"""
        initial_rows = len(df)

        # 1. 构造原始文件路径，用于删除
        today_str = self.fetcher.today_str
        original_file_name = f"{df_name}_{today_str}.txt"
        original_file_path = os.path.join(self.fetcher.config.TEMP_DATA_DIRECTORY, original_file_name)

        # 2. 执行清洗和标准化
        df = self.standardize_columns(df)
        if df.empty or '股票代码' not in df.columns:
            print(f"[WARN] {df_name} 数据标准化失败或为空，跳过清洗。")
            return pd.DataFrame()

        df.dropna(subset=['股票代码'], inplace=True)
        df.drop_duplicates(subset=['股票代码'], inplace=True)
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)

        # 优化：在清洗前添加一个临时的股票简称列，以防缺失，方便ST过滤
        if '股票简称' not in df.columns:
            df['股票简称'] = df['股票代码'].astype(str)  # 临时使用代码

        cleaned_df = df[~df['股票简称'].str.contains('ST|st|退市', case=False, na=False)].copy()

        # 恢复原始的股票简称列（如果临时添加了）
        if '股票简称' in df.columns and cleaned_df.columns.tolist()[
            -1] == '股票简称' and '股票简称' not in self.name_aliases.values():
            pass

        final_rows = len(cleaned_df)
        print(f"{df_name} 清洗完成。清洗前：{initial_rows} 条，清洗后：{final_rows} 条。")

        # 3. 构造并保存清洗后的文件
        cleaned_file_name = f"{df_name}_经清洗_{today_str}.txt"
        cleaned_file_path = os.path.join(self.fetcher.config.TEMP_DATA_DIRECTORY, cleaned_file_name)
        self.fetcher.save_data_to_txt(cleaned_df, cleaned_file_path)

        # 4. 删除原始文件
        try:
            # 只有当原始文件存在（即未命中清洗缓存）时才删除
            if os.path.exists(original_file_path):
                os.remove(original_file_path)
                print(f"已删除原始临时文件: {original_file_name}")
        except Exception as e:
            print(f"[WARN] 警告：删除原始文件 {original_file_name} 失败: {e}")

        return cleaned_df

    def process_profit_data(self, df: pd.DataFrame, min_rating: int = 2) -> pd.DataFrame:
        df = self.clean_data(df, "主力研报盈利预测")
        if df.empty:
            return pd.DataFrame()
        df['机构投资评级(近六个月)-买入'] = pd.to_numeric(df['机构投资评级(近六个月)-买入'], errors='coerce')
        df = df[df['机构投资评级(近六个月)-买入'] >= min_rating].copy()
        df['完整股票编码'] = df['股票代码'].apply(format_stock_code)
        print(f"研报数据过滤完成，符合条件的股票数量: {len(df)}")
        return df

    def process_main_report_sheet(self, profit_df: pd.DataFrame, spot_df: pd.DataFrame) -> pd.DataFrame:
        """生成“主力研报筛选” Sheet 的数据。"""
        if profit_df.empty or spot_df.empty:
            print("[WARN] 研报数据或实时行情数据为空，无法生成主力研报筛选表。")
            return pd.DataFrame()
        # 注意：这里 spot_df 中使用的价格列名是 '最新价'
        final_df = pd.merge(profit_df, spot_df[['股票代码', '最新价']], on='股票代码', how='left')
        final_df['最新价'] = final_df['最新价'].fillna('N/A')
        final_df['股票链接'] = 'https://hybrid.gelonghui.com/stock-check/' + final_df['完整股票编码'].astype(
            str).str.lower()
        final_cols = ['股票代码', '完整股票编码', '股票链接', '最新价']
        other_cols = [col for col in final_df.columns if col not in final_cols]
        return final_df[final_cols + other_cols]

    def process_spot_data(self, spot_data_all: pd.DataFrame, filtered_codes_df: pd.DataFrame) -> pd.DataFrame:
        """
        处理实时行情数据，确保价格列名为'当前价格'，并标准化 '市盈率-动态' 字段。
        """
        # 实时行情数据清洗
        spot_data_all = self.clean_data(spot_data_all, "A股实时行情")

        # 还需要处理备用接口的数据，如果存在的话
        spot_data_fallback = self.fetcher.fetch_with_cache(ak.stock_zh_a_spot, 'A股实时行情_备用')
        if not spot_data_fallback.empty:
            spot_data_fallback = self.clean_data(spot_data_fallback, 'A股实时行情_备用')
            # 将备用数据与主数据合并，优先保留主数据
            spot_data_all = pd.concat([spot_data_all, spot_data_fallback]).drop_duplicates(subset=['股票代码'],
                                                                                           keep='first')

        if spot_data_all.empty or filtered_codes_df.empty:
            return pd.DataFrame()

        # 核心修复：确保价格列名为'当前价格'
        if '最新价' in spot_data_all.columns:
            spot_data_all.rename(columns={'最新价': '当前价格'}, inplace=True)
        elif '现价' in spot_data_all.columns:
            spot_data_all.rename(columns={'现价': '当前价格'}, inplace=True)
        
        # 【修改点 A: 确保并标准化动态市盈率字段】
        if '市盈率-动态' in spot_data_all.columns:
            spot_data_all.rename(columns={'市盈率-动态': '动态市盈率'}, inplace=True)
        else:
            # 如果没有找到，则创建空列，防止后续合并失败
            spot_data_all['动态市盈率'] = np.nan


        # 确保合并后保留'股票代码'、'当前价格'和'动态市盈率'
        cols_to_keep = ['股票代码', '股票简称', '当前价格', '动态市盈率']
        
        # 仅保留清洗后数据中包含的列
        cols_to_keep_final = [col for col in cols_to_keep if col in spot_data_all.columns]

        # 仅合并需要的列
        filtered_spot_data = pd.merge(
            spot_data_all[cols_to_keep_final], 
            filtered_codes_df[['股票代码', '完整股票编码']], 
            on='股票代码',
            how='inner'
        )
        
        return filtered_spot_data

    def process_financial_abstract(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理财务摘要数据，进行清洗和格式化。"""
        return self.clean_data(df, "财务摘要数据")

    def process_market_fund_flow(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理市场资金流向数据 (5日排行)。
        """
        df = self.clean_data(df, "市场资金流向")
        if df.empty:
            print("警告：未能获取市场资金流向数据。")
            return pd.DataFrame()
        # 按照“流入资金”字段倒序排序
        if '流入资金' in df.columns:
            df['流入资金'] = pd.to_numeric(df['流入资金'], errors='coerce')
            df = df.sort_values(by='流入资金', ascending=False).copy()
        else:
            print("警告：未能找到 '流入资金' 列进行排序。")
        print(f"  - 市场资金流向数据处理成功，共 {len(df)} 条。")
        return df

    def process_general_rank(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """通用排行榜数据处理，添加股票代码和编码。"""
        # 注意：这里 name 参数是 file_base_name
        cleaned_df = self.clean_data(df, name)
        if not cleaned_df.empty and '股票代码' in cleaned_df.columns:
            # 对于排行榜数据，可能需要添加完整股票编码
            cleaned_df['完整股票编码'] = cleaned_df['股票代码'].apply(format_stock_code)
        return cleaned_df

    def process_board_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """通用板块数据处理，进行清洗和标准化。"""
        # 注意：这里的 df_name 要传入 ak.stock_rank_xstp_ths 对应的 base_name
        return self.clean_data(df, "板块数据")

    def process_and_merge_xstp_data(self, df20: pd.DataFrame, df60: pd.DataFrame, df90: pd.DataFrame,
                                    spot_data_all: pd.DataFrame) -> pd.DataFrame:
        """处理并合并20日、60日和90日均线数据，并添加实时价格过滤。"""
        print("正在处理并合并20日、60日和90日均线数据...")

        # 使用 clean_data 并传入正确的 file_base_name
        processed_df20 = self.clean_data(df20, '向上突破20日均线').rename(columns={'最新价': '20日均线最新价'})
        processed_df60 = self.clean_data(df60, '向上突破60日均线').rename(columns={'最新价': '60日均线最新价'})
        processed_df90 = self.clean_data(df90, '向上突破90日均线').rename(columns={'最新价': '90日均线最新价'})

        if processed_df20.empty and processed_df60.empty and processed_df90.empty:
            print("[WARN] 所有均线数据均为空，无法合并。")
            return pd.DataFrame()

        # 初始合并
        merged_df = processed_df20[['股票代码', '股票简称', '20日均线最新价']].copy()

        # 合并 60日数据
        if not processed_df60.empty:
            merged_df = pd.merge(merged_df, processed_df60[['股票代码', '股票简称', '60日均线最新价']],
                                 on='股票代码', how='outer', suffixes=('_x', '_y'))
            merged_df['股票简称'] = merged_df['股票简称_x'].fillna(merged_df['股票简称_y'])
            merged_df.drop(columns=[c for c in ['股票简称_x', '股票简称_y'] if c in merged_df.columns], inplace=True)
            merged_df.drop_duplicates(subset=['股票代码'], inplace=True)

        # 合并 90日数据
        if not processed_df90.empty:
            merged_df = pd.merge(merged_df, processed_df90[['股票代码', '股票简称', '90日均线最新价']],
                                 on='股票代码', how='outer', suffixes=('_x', '_y'))
            merged_df['股票简称'] = merged_df['股票简称_x'].fillna(merged_df['股票简称_y'])
            merged_df.drop(columns=[c for c in ['股票简称_x', '股票简称_y'] if c in merged_df.columns], inplace=True)
            merged_df.drop_duplicates(subset=['股票代码'], inplace=True)

        final_cols = ['股票代码', '股票简称', '20日均线最新价', '60日均线最新价', '90日均线最新价']
        final_merged_df = merged_df[[col for col in final_cols if col in merged_df.columns]].copy()
        print("正在将实时价格合并到均线数据集中...")

        # 确保用于合并的spot_data_all DataFrame包含正确的列名
        spot_data_all_temp = spot_data_all.copy()
        if '最新价' in spot_data_all_temp.columns:
            spot_data_all_temp.rename(columns={'最新价': '当前价格'}, inplace=True)
        elif '现价' in spot_data_all_temp.columns:
            spot_data_all_temp.rename(columns={'现价': '当前价格'}, inplace=True)

        final_merged_df = pd.merge(final_merged_df, spot_data_all_temp[['股票代码', '当前价格']], on='股票代码',
                                   how='left')

        # 类型转换
        final_merged_df['20日均线最新价'] = pd.to_numeric(final_merged_df['20日均线最新价'], errors='coerce')
        final_merged_df['60日均线最新价'] = pd.to_numeric(final_merged_df['60日均线最新价'], errors='coerce')
        final_merged_df['90日均线最新价'] = pd.to_numeric(final_merged_df['90日均线最新价'], errors='coerce')
        final_merged_df['当前价格'] = pd.to_numeric(final_merged_df['当前价格'], errors='coerce')

        # 集中执行所有过滤条件:
        # 1. 名称不为空 2. 20日均线不为空 3. 当前价格不为空 4. 当前价格 > 20日均线
        filtered_df = final_merged_df[
            (final_merged_df['股票简称'].notna()) &
            (final_merged_df['20日均线最新价'].notna()) &
            (final_merged_df['当前价格'].notna()) &
            (final_merged_df['当前价格'] > final_merged_df['20日均线最新价'])
            ].copy()

        # 5. 添加多头排列条件 (20日>60日 或 60日>90日)
        # 按照原代码的逻辑实现：
        filtered_df = filtered_df[
            (filtered_df['20日均线最新价'] > filtered_df['60日均线最新价'].fillna(-np.inf)) |
            (filtered_df['60日均线最新价'] > filtered_df['90日均线最新价'].fillna(-np.inf))
            ].copy()

        # 6. 完全多头排列 (20日>60日>90日) - 可以在这里添加一个额外的列进行标记
        # 确保非空后再比较
        filtered_df['完全多头排列'] = filtered_df.apply(
            lambda row: '是' if row['20日均线最新价'] > row['60日均线最新价'] and row['60日均线最新价'] > row[
                '90日均线最新价'] else '否',
            axis=1
        )

        filtered_df.fillna('N/A', inplace=True)
        print(f"均线数据合并与过滤完成，符合条件的股票数量: {len(filtered_df)}")
        return filtered_df

    def process_all_technical_indicators(self, all_ta_codes: list, hist_df_all: pd.DataFrame,
                                         source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """集中处理所有技术指标，避免重复计算。"""
        print(f"\n正在对 {len(all_ta_codes)} 只股票进行批量技术分析...")
        # 新增 ADX 和 BOLL 的结果列表
        results = {'macd': [], 'cci': [], 'rsi': [], 'adx': [], 'boll': []}
        grouped = hist_df_all.groupby('股票代码')

        # 预先清理股票信息，用于查找简称
        source_df_clean = source_df[['股票代码', '股票简称']].drop_duplicates(subset=['股票代码'])

        for code, group_df in grouped:
            try:
                # 至少需要30条数据才能计算MACD/RSI/CCI
                if len(group_df) < 30:
                    continue

                # 确保列名标准化
                group_df.rename(columns={'收盘': 'close', '最高': 'high', '最低': 'low'}, inplace=True)

                # --- MACD, CCI, RSI 计算 (使用 module syntax) ---
                macd_cols = ta.macd(group_df['close'], append=False)
                cci_cols = ta.cci(group_df['high'], group_df['low'], group_df['close'], append=False)
                rsi6_cols = ta.rsi(group_df['close'], length=6, append=False).rename('RSI_6')
                rsi14_cols = ta.rsi(group_df['close'], length=14, append=False).rename('RSI_14')

                # --- 新增 BOLL 计算 (默认长度20, 标准差2) ---
                boll_cols = ta.bbands(group_df['close'], append=False)

                # --- 新增 ADX 计算 (默认长度14) ---
                adx_cols = ta.adx(group_df['high'], group_df['low'], group_df['close'], append=False)

                # 安全地连接所有指标结果
                group_df = pd.concat([group_df, macd_cols, cci_cols, rsi6_cols, rsi14_cols, boll_cols, adx_cols],
                                     axis=1)

                # --- MACD 信号检查 ---
                if len(group_df) >= 2 and 'MACD_12_26_9' in group_df.columns:
                    last_day_macd = group_df.iloc[-1]
                    prev_day_macd = group_df.iloc[-2]

                    is_golden_cross = (prev_day_macd['MACD_12_26_9'] < prev_day_macd['MACDs_12_26_9']) and (
                            last_day_macd['MACD_12_26_9'] > last_day_macd['MACDs_12_26_9'])

                    if is_golden_cross:
                        stock_info = source_df_clean[source_df_clean['股票代码'] == code].iloc[0]
                        results['macd'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'MACD (DIF)': f"{last_day_macd['MACD_12_26_9']:.2f}",
                            'MACD信号线 (DEA)': f"{last_day_macd['MACDs_12_26_9']:.2f}",
                            'MACD动能柱': f"{last_day_macd['MACDh_12_26_9']:.2f}",
                            'MACD买卖信号': '金叉 (买入信号)',
                        })

                # --- CCI 信号检查 ---
                if len(group_df) >= 2 and 'CCI_14_0.015' in group_df.columns:
                    last_day_cci = group_df.iloc[-1]
                    prev_day_cci = group_df.iloc[-2]

                    is_oversold_signal = prev_day_cci['CCI_14_0.015'] < -100 and last_day_cci['CCI_14_0.015'] > -100

                    if is_oversold_signal:
                        stock_info = source_df_clean[source_df_clean['股票代码'] == code].iloc[0]
                        results['cci'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新CCI值': f"{last_day_cci['CCI_14_0.015']:.2f}",
                            'CCI买卖信号': '超卖买入信号',
                        })

                # --- RSI 信号检查 ---
                if len(group_df) >= 2 and 'RSI_6' in group_df.columns and 'RSI_14' in group_df.columns:
                    last_day_rsi = group_df.iloc[-1]
                    prev_day_rsi = group_df.iloc[-2]

                    is_golden_cross_rsi = (prev_day_rsi['RSI_6'] < prev_day_rsi['RSI_14']) and (
                            last_day_rsi['RSI_6'] > last_day_rsi['RSI_14'])
                    is_rsi14_in_range = 60 <= last_day_rsi['RSI_14'] <= 80

                    if is_golden_cross_rsi and is_rsi14_in_range:
                        stock_info = source_df_clean[source_df_clean['股票代码'] == code].iloc[0]
                        results['rsi'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'RSI6': round(last_day_rsi['RSI_6'], 2),
                            'RSI14': round(last_day_rsi['RSI_14'], 2),
                            'RSI买卖信号': '金叉 (买入信号)',
                        })

                # --- 新增 BOLL 信号检查 ---
                if 'BBL_20_2.0' in group_df.columns:
                    last_day = group_df.iloc[-1]

                    # 价格差与带宽的比值（判断价格是否在下轨线附近）
                    price_diff_to_lower = last_day['close'] - last_day['BBL_20_2.0']
                    band_width = last_day['BBU_20_2.0'] - last_day['BBL_20_2.0']

                    # 检查 1：价格刚触及下轨（收盘价高于下轨线，且与下轨线的距离小于带宽的 5%）
                    is_near_lower_band = (last_day['close'] > last_day['BBL_20_2.0']) and \
                                         (price_diff_to_lower < (band_width * 0.05))

                    # 检查 2：波动率收窄 (带宽/中轨线 < 5%)
                    is_low_volatility = (band_width / last_day['BBM_20_2.0']) < 0.05

                    if is_near_lower_band and is_low_volatility:
                        stock_info = source_df_clean[source_df_clean['股票代码'] == code].iloc[0]
                        results['boll'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新价格': f"{last_day['close']:.2f}",
                            '下轨线': f"{last_day['BBL_20_2.0']:.2f}",
                            'BOLL买卖信号': '下轨附近低波动买入',
                        })

                # --- 新增 ADX 信号检查 ---
                if 'ADX_14' in group_df.columns:
                    last_day_adx = group_df.iloc[-1]
                    last_adx_value = last_day_adx['ADX_14']

                    # 检查：趋势强度处于强势区间 (> 25)
                    is_strong_trend_and_rising = last_adx_value > 25

                    if is_strong_trend_and_rising:
                        stock_info = source_df_clean[source_df_clean['股票代码'] == code].iloc[0]
                        results['adx'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新ADX值': f"{last_adx_value:.2f}",
                            'ADX买卖信号': '趋势强势确认 (>25)',
                        })

            except Exception as e:
                print(f"[ERROR] 错误：计算 {code} 的技术指标时出错: {e}，已跳过。")

        # 组装最终结果
        macd_df = pd.DataFrame(results['macd']) if results['macd'] else pd.DataFrame()
        cci_df = pd.DataFrame(results['cci']) if results['cci'] else pd.DataFrame()
        rsi_df = pd.DataFrame(results['rsi']) if results['rsi'] else pd.DataFrame()
        adx_df = pd.DataFrame(results['adx']) if results['adx'] else pd.DataFrame()
        boll_df = pd.DataFrame(results['boll']) if results['boll'] else pd.DataFrame()

        print(
            f"MACD金叉: {len(macd_df)} 只，CCI超卖: {len(cci_df)} 只，RSI金叉: {len(rsi_df)} 只，ADX强势: {len(adx_df)} 只，BOLL低波: {len(boll_df)} 只。")
        return {'macd_df': macd_df, 'cci_df': cci_df, 'rsi_df': rsi_df, 'adx_df': adx_df, 'boll_df': boll_df}

    def find_recommended_stocks_with_score(self, macd_df: pd.DataFrame, cci_df: pd.DataFrame, xstp_df: pd.DataFrame,
                                           rsi_df: pd.DataFrame, strong_stocks_df: pd.DataFrame,
                                           filtered_spot: pd.DataFrame,
                                           consecutive_rise_df: pd.DataFrame,
                                           adx_df: pd.DataFrame, boll_df: pd.DataFrame,
                                           ljqs_df: pd.DataFrame) -> pd.DataFrame:
        """基于多因子评分筛选推荐股票。增加了 ljqs_df (量价齐升) 参数。"""
        print("\n正在基于多因子评分筛选推荐股票...")

        # 1. 将所有指标加入到待合并列表
        input_dfs = [macd_df, cci_df, xstp_df, rsi_df, adx_df, boll_df, ljqs_df]

        df_to_concat = []
        for df in input_dfs:
            # 只有当 DataFrame 不为空且包含 '股票代码' 和 '股票简称' 时才合并
            if not df.empty and '股票代码' in df.columns and '股票简称' in df.columns:
                df_to_concat.append(df[['股票代码', '股票简称']].copy())

        if not df_to_concat:
            print("[WARN] 未找到任何符合任一条件的股票。")
            # 返回包含所有可能列的空 DataFrame，方便后续操作
            return pd.DataFrame(columns=['股票代码', '股票简称', '动态市盈率', '符合条件数量', 'MACD买卖信号', 
                                         'CCI买卖信号', 'RSI买卖信号', '均线多头排列',
                                         'ADX趋势强度', 'BOLL波动性信号', '量价齐升信号',  # 新增列
                                         '强势股池', '连涨天数', '当前价格', '股票链接'])

        all_codes = pd.concat(df_to_concat, ignore_index=True).drop_duplicates()

        if all_codes.empty:
            print("[WARN] 未找到任何符合任一条件的股票。")
            return pd.DataFrame()

        all_codes = all_codes[~all_codes['股票简称'].str.contains('ST|st|退市', case=False, na=False)].copy()
        final_df = all_codes.copy()

        # 初始化评分列和信号列
        final_df['符合条件数量'] = 0
        final_df['MACD买卖信号'] = '未满足'
        final_df['CCI买卖信号'] = '未满足'
        final_df['RSI买卖信号'] = '未满足'
        final_df['均线多头排列'] = '未满足'
        final_df['ADX趋势强度'] = '未满足'
        final_df['BOLL波动性信号'] = '未满足'
        final_df['量价齐升信号'] = '未满足'  # 新增初始化 量价齐升 列
        final_df['动态市盈率'] = np.nan       # 【修改点 B: 初始化动态市盈率列】


        def update_df(source_df: pd.DataFrame, column_name: str, check_col: str = None):
            if '股票代码' not in source_df.columns:
                return

            for code in source_df['股票代码'].unique():
                if code in final_df['股票代码'].values:
                    # 递增计数
                    final_df.loc[final_df['股票代码'] == code, '符合条件数量'] += 1

                    # 更新信号列
                    if check_col and check_col in source_df.columns:
                        signal_val = source_df[source_df['股票代码'] == code].iloc[0][check_col]
                        final_df.loc[final_df['股票代码'] == code, column_name] = signal_val
                    else:
                        final_df.loc[final_df['股票代码'] == code, column_name] = '已满足'

        update_df(macd_df, 'MACD买卖信号', 'MACD买卖信号')
        update_df(cci_df, 'CCI买卖信号', 'CCI买卖信号')
        update_df(rsi_df, 'RSI买卖信号', 'RSI买卖信号')
        update_df(xstp_df, '均线多头排列')
        update_df(adx_df, 'ADX趋势强度', 'ADX买卖信号')
        update_df(boll_df, 'BOLL波动性信号', 'BOLL买卖信号')
        # 新增 量价齐升 的更新
        if not ljqs_df.empty:
            # 量价齐升的信号列可以显示“量价齐升天数”
            ljqs_df['量价齐升信号'] = '量价齐升: ' + ljqs_df['量价齐升天数'].astype(str) + '天'
            update_df(ljqs_df, '量价齐升信号', '量价齐升信号')


        # 强势股池
        strong_stocks_codes = set(strong_stocks_df[
                                      '股票代码']) if not strong_stocks_df.empty and '股票代码' in strong_stocks_df.columns else set()
        final_df['强势股池'] = final_df['股票代码'].apply(lambda x: 'YES' if x in strong_stocks_codes else 'NO')

        # 新增：合并连涨天数数据
        if not consecutive_rise_df.empty and '连涨天数' in consecutive_rise_df.columns and '股票代码' in consecutive_rise_df.columns:
            consecutive_rise_df_temp = consecutive_rise_df[['股票代码', '连涨天数']].copy()
            consecutive_rise_df_temp['连涨天数'] = pd.to_numeric(consecutive_rise_df_temp['连涨天数'], errors='coerce')
            final_df = pd.merge(final_df, consecutive_rise_df_temp, on='股票代码', how='left')
            final_df['连涨天数'] = final_df['连涨天数'].fillna(0).astype(int)
        else:
            final_df['连涨天数'] = 0

        # 合并最新价，使用filtered_spot中的 '当前价格' 列
        if '当前价格' in filtered_spot.columns:
            final_df = pd.merge(final_df, filtered_spot[['股票代码', '当前价格']], on='股票代码', how='left')
        else:
            final_df['当前价格'] = 'N/A'
            
        final_df['当前价格'] = final_df['当前价格'].fillna('N/A')

        # 【修改点 C: 合并动态市盈率数据】
        if '动态市盈率' in filtered_spot.columns:
            # 仅在 final_df 中还未被填充的地方，从 filtered_spot 获取数据
            final_df.drop(columns=['动态市盈率'], inplace=True, errors='ignore') # 移除初始化的np.nan列
            final_df = pd.merge(final_df, filtered_spot[['股票代码', '动态市盈率']], on='股票代码', how='left')
            # 格式化动态市盈率
            final_df['动态市盈率'] = final_df['动态市盈率'].apply(
                lambda x: f"{float(x):.2f}" if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else 'N/A')
        else:
            final_df['动态市盈率'] = 'N/A'


        # 增加完整股票编码列用于链接生成
        final_df['完整股票编码'] = final_df['股票代码'].apply(format_stock_code)

        final_df['股票链接'] = final_df['完整股票编码'].apply(
            lambda x: f'https://hybrid.gelonghui.com/stock-check/{x.lower()}' if x != 'N/A' else 'N/A')

        final_df.drop(columns=['完整股票编码'], inplace=True)

        recommended_df = final_df[final_df['符合条件数量'] >= 1].sort_values(by='符合条件数量',
                                                                             ascending=False).reset_index(
            drop=True)
        # 将 NaN 填充为 N/A (除了价格和市盈率，它们已在上面处理或格式化)
        for col in ['MACD买卖信号', 'CCI买卖信号', 'RSI买卖信号', '均线多头排列', 'ADX趋势强度', 'BOLL波动性信号', '量价齐升信号']:
            recommended_df[col] = recommended_df[col].fillna('未满足')
        recommended_df.fillna('N/A', inplace=True)

        # 确保列顺序
        final_cols_order = ['股票代码', '股票简称', '动态市盈率', # 【修改点 D: 动态市盈率前置】
                            '符合条件数量', 'MACD买卖信号', 'CCI买卖信号', 'RSI买卖信号',
                            '均线多头排列', 'ADX趋势强度', 'BOLL波动性信号', '量价齐升信号', 
                            '强势股池', '连涨天数', '当前价格', '股票链接']

        # 仅保留在 DataFrame 中存在的列
        final_cols_order = [col for col in final_cols_order if col in recommended_df.columns]
        recommended_df = recommended_df[final_cols_order]

        print(f"成功筛选出 {len(recommended_df)} 只最终推荐股票，并按符合条件数量排序。")
        return recommended_df


# ==============================================================================
# Excel报告生成类
# ==============================================================================
class ExcelReporter:
    """
    负责将处理后的数据导出为结构化的Excel报告。
    """

    def __init__(self, config: Config):
        self.config = config
        self.file_path = os.path.join(self.config.SAVE_DIRECTORY,
                                      f"主力研报筛选_{datetime.now().strftime('%Y%m%d')}.xlsx")
        # 确保文件存在且可写入
        try:
            self.writer = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
            self.workbook = self.writer.book
            self.header_format = self.workbook.add_format(
                {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
            self.text_format = self.workbook.add_format({'border': 1})
            self.link_format = self.workbook.add_format({'border': 1, 'font_color': 'blue', 'underline': 1})
            self.red_format = self.workbook.add_format({'border': 1, 'font_color': 'red'})
            self.green_format = self.workbook.add_format({'border': 1, 'font_color': 'green'})
            self.yellow_format = self.workbook.add_format({'border': 1, 'bg_color': '#FFFF00'})  # 新增黄色格式
            self.blue_format = self.workbook.add_format({'border': 1, 'font_color': 'blue'}) # 新增蓝色格式
        except Exception as e:
            print(f"[FATAL] 错误：无法初始化 Excel 写入器: {e}")
            self.writer = None
            self.workbook = None
            raise

    def _write_dataframe(self, df: pd.DataFrame, sheet_name: str, link_col: str = None,
                         conditional_format: Dict[str, Any] = None):
        """通用写入DataFrame到Excel的方法。"""
        if df.empty:
            print(f"[WARN] {sheet_name} 数据为空，跳过生成该工作表。")
            return
        if self.writer is None or self.workbook is None:
            print(f"[ERROR] Excel 写入器未正确初始化，跳过写入 {sheet_name}。")
            return

        try:
            # 确保 sheet name 不超过 31 个字符
            safe_sheet_name = sheet_name[:31]
            worksheet = self.workbook.add_worksheet(safe_sheet_name)

            # 使用 to_excel 写入数据，不带表头和索引
            df.to_excel(self.writer, sheet_name=safe_sheet_name, index=False, startrow=1, header=False)

            # 写入表头并设置列宽
            for col_num, value in enumerate(df.columns):
                worksheet.write(0, col_num, value, self.header_format)
                # 尝试根据内容设置一个合理的默认宽度
                max_len = max(df[value].astype(str).apply(len).max(), len(value)) if not df.empty else len(value)
                worksheet.set_column(col_num, col_num, max(15, min(30, max_len + 2)), self.text_format)

            # 处理链接列
            if link_col and link_col in df.columns:
                link_col_idx = df.columns.get_loc(link_col)
                for row_num, link in enumerate(df[link_col], 1):
                    try:
                        if link and link not in ('N/A', '链接无效'):
                            # 写入链接，显示文本为 '链接' 或 '点击链接'
                            display_text = link if sheet_name == '指标汇总' else '链接'
                            worksheet.write_url(row_num, link_col_idx, str(link), self.link_format, display_text)
                        else:
                            worksheet.write(row_num, link_col_idx, 'N/A', self.text_format)
                    except xlsxwriter.exceptions.XlsxWriterException as e:
                        # 链接太长或格式错误
                        worksheet.write(row_num, link_col_idx, '链接无效', self.text_format)

            # 处理条件格式
            if conditional_format:
                for condition in conditional_format:
                    col_name = condition['column']
                    if col_name in df.columns:
                        col_idx = df.columns.get_loc(col_name)
                        for row_num, value in enumerate(df[col_name], 1):
                            # 使用 try-except 来处理可能出现的格式化错误
                            try:
                                # 检查条件是否满足
                                if condition['check'](value):
                                    worksheet.write(row_num, col_idx, value, condition['format'])
                                else:
                                    # 确保没有格式的单元格仍然有边框
                                    worksheet.write(row_num, col_idx, value, self.text_format)
                            except Exception:
                                worksheet.write(row_num, col_idx, value, self.text_format)  # 格式错误时保持默认

        except Exception as e:
            print(f"[ERROR] 写入工作表 {safe_sheet_name} 时出错: {e}")

    def generate_report(self, sheets_data: Dict[str, pd.DataFrame]):
        """生成最终的Excel报告。"""
        if self.writer is None or self.workbook is None:
            print("[FATAL] 无法生成报告，Excel 写入器未初始化。")
            return

        print("\n>>> 正在生成Excel报告...")
        sheet_specs = {
            '指标汇总': {'df': sheets_data.get('指标汇总'), 'link_col': '股票链接', 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format},
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'ADX趋势强度', 'check': lambda x: '强势' in str(x), 'format': self.red_format},
                {'column': 'BOLL波动性信号', 'check': lambda x: '低波动买入' in str(x), 'format': self.yellow_format},
                # 新增 量价齐升 的条件格式：突出显示
                {'column': '量价齐升信号', 'check': lambda x: '量价齐升' in str(x), 'format': self.blue_format}
            ]},
            '主力研报筛选': {'df': sheets_data.get('主力研报筛选'), 'link_col': '股票链接', 'conditional_format': None},
            '财务摘要数据': {'df': sheets_data.get('财务摘要数据'), 'link_col': None, 'conditional_format': None},
            '实时行情': {'df': sheets_data.get('实时行情'), 'link_col': None, 'conditional_format': None},
            '行业板块': {'df': sheets_data.get('行业板块'), 'link_col': None, 'conditional_format': None},
            '市场资金流向': {'df': sheets_data.get('市场资金流向'), 'link_col': None, 'conditional_format': None},
            '前十板块成分股': {'df': sheets_data.get('前十板块成分股'), 'link_col': None, 'conditional_format': None},
            '均线多头排列': {'df': sheets_data.get('均线多头排列'), 'link_col': None, 'conditional_format': None},
            '向上突破': {'df': sheets_data.get('向上突破'), 'link_col': None, 'conditional_format': None},
            '强势股池': {'df': sheets_data.get('强势股池'), 'link_col': None, 'conditional_format': None},
            '连续上涨': {'df': sheets_data.get('连续上涨'), 'link_col': None, 'conditional_format': None},
            'MACD金叉': {'df': sheets_data.get('MACD金叉'), 'link_col': None, 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            'CCI超卖': {'df': sheets_data.get('CCI超卖'), 'link_col': None, 'conditional_format': [
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format}]},
            'RSI金叉': {'df': sheets_data.get('RSI金叉'), 'link_col': None, 'conditional_format': [
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            'ADX强势': {'df': sheets_data.get('ADX强势'), 'link_col': None, 'conditional_format': [
                {'column': 'ADX买卖信号', 'check': lambda x: '强势' in str(x), 'format': self.red_format}]},
            'BOLL低波': {'df': sheets_data.get('BOLL低波'), 'link_col': None, 'conditional_format': [
                {'column': 'BOLL买卖信号', 'check': lambda x: '买入' in str(x), 'format': self.yellow_format}]},
            # 新增工作表
            '量价齐升': {'df': sheets_data.get('量价齐升'), 'link_col': None, 'conditional_format': [
                {'column': '量价齐升天数', 'check': lambda x: pd.to_numeric(x, errors='coerce') >= 3, 'format': self.blue_format}]},
        }
        try:
            for sheet_name, spec in sheet_specs.items():
                if spec['df'] is not None:
                    self._write_dataframe(spec['df'], sheet_name, spec.get('link_col'), spec.get('conditional_format'))
            print(f"报告已成功生成: {self.file_path}")
        except Exception as e:
            print(f"[ERROR] 生成Excel报告时出错: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """关闭Excel写入器。"""
        if self.writer:
            try:
                self.writer.close()
            except Exception as e:
                print(f"[ERROR] 关闭Excel写入器时出错: {e}")


# ==============================================================================
# 主流程调度类
# ==============================================================================
class StockDataPipeline:
    """
    协调整个数据获取、处理和报告生成流程。
    """

    def __init__(self, config: Config = None):
        self.config = config if config else Config()
        self.fetcher = DataFetcher(self.config)
        self.processor = DataProcessor(self.fetcher)
        self.reporter = ExcelReporter(self.config)

    def run(self):
        """执行整个分析流程。"""
        start_time = time.time()
        print(f">>> 股票数据分析流程启动... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            print("\n>>> 正在获取基础数据...")

            # --- 实时行情数据获取及备用接口逻辑 ---
            # 尝试主接口
            spot_data_all = self.fetcher.fetch_with_cache(ak.stock_zh_a_spot_em, 'A股实时行情')

            if spot_data_all.empty:
                print(
                    "\n[WARN] 警告：使用接口 ak.stock_zh_a_spot_em 获取A股实时行情失败，尝试使用备用接口 ak.stock_zh_a_spot。")
                # 尝试备用接口
                spot_data_all_fallback = self.fetcher.fetch_with_cache(ak.stock_zh_a_spot, 'A股实时行情_备用')

                # 如果备用接口成功，则使用备用数据
                if not spot_data_all_fallback.empty:
                    spot_data_all = spot_data_all_fallback

            if spot_data_all.empty:
                print("\n[ERROR] 错误：所有实时行情接口均失败，后续流程可能受影响。")
            # ----------------------------------------

            # 获取其他原始数据
            profit_data_raw = self.fetcher.fetch_with_cache(ak.stock_profit_forecast_em, '主力研报盈利预测')
            market_fund_flow_raw = self.fetcher.fetch_with_cache(ak.stock_fund_flow_individual, '市场资金流向',
                                                                 symbol="5日排行")
            industry_board_df = self.fetcher.fetch_with_cache(ak.stock_board_industry_name_em, '行业板块名称')
            # financial_abstract_df = self.fetcher.fetch_with_cache(ak.stock_financial_abstract, '财务摘要数据') # PEG计算逻辑已移除
            strong_stocks_df_raw = self.fetcher.fetch_with_cache(ak.stock_rank_ljqd_ths, '强势股池')
            consecutive_rise_df_raw = self.fetcher.fetch_with_cache(ak.stock_rank_lxsz_ths, '连续上涨')
            df_ma20 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破20日均线', symbol="20日均线")
            df_ma60 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破60日均线', symbol="60日均线")
            df_ma90 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破90日均线', symbol="90日均线")
            # 新增 量价齐升 数据获取
            ljqs_df_raw = self.fetcher.fetch_with_cache(ak.stock_rank_ljqs_ths, '量价齐升')


            print("\n>>> 正在进行数据处理和筛选...")

            # --- 清洗和预处理 ---
            processed_profit_data = self.processor.process_profit_data(profit_data_raw)
            # 这里的 processed_spot_data_cleaned 只是用于生成研报筛选表的"最新价"
            processed_spot_data_cleaned = self.processor.clean_data(spot_data_all, "A股实时行情") 
            
            # filtered_spot 包含了 '当前价格' 和 '动态市盈率' 
            filtered_spot = self.processor.process_spot_data(spot_data_all, processed_profit_data)

            main_report_sheet = self.processor.process_main_report_sheet(processed_profit_data,
                                                                         processed_spot_data_cleaned)
            # processed_financial_abstract = self.processor.process_financial_abstract(financial_abstract_df) # PEG计算逻辑已移除
            processed_market_fund_flow = self.processor.process_market_fund_flow(market_fund_flow_raw)
            processed_strong_stocks = self.processor.process_general_rank(strong_stocks_df_raw, '强势股池')
            processed_consecutive_rise = self.processor.process_general_rank(consecutive_rise_df_raw, '连续上涨')
            # 新增 量价齐升 数据清洗
            processed_ljqs = self.processor.process_general_rank(ljqs_df_raw, '量价齐升')

            # 使用新的获取方法
            top_industry_cons_df = self.fetcher.get_top_industry_stocks()
            processed_xstp_df = self.processor.process_and_merge_xstp_data(df_ma20, df_ma60, df_ma90,
                                                                           processed_spot_data_cleaned)

            # --- 技术指标计算 ---
            # 合并所有需要进行技术分析的股票代码，只获取一次历史数据
            all_ta_codes = set(processed_profit_data['股票代码'].tolist())
            if not processed_xstp_df.empty:
                all_ta_codes.update(processed_xstp_df['股票代码'].tolist())
            
            # 增加量价齐升的股票代码到技术分析列表，便于查找股票简称
            if not processed_ljqs.empty:
                all_ta_codes.update(processed_ljqs['股票代码'].tolist())

            all_ta_codes = [code for code in all_ta_codes if pd.notna(code)]

            # 用于技术指标名称查找的源DataFrame
            # 注意：这里只需要股票代码和简称，其他实时数据（如PE）直接从 filtered_spot 获取
            ta_source_df = pd.concat([processed_profit_data[['股票代码', '股票简称']].drop_duplicates(),
                                      processed_xstp_df[['股票代码', '股票简称']].drop_duplicates(),
                                      processed_ljqs[['股票代码', '股票简称']].drop_duplicates()], 
                                     ignore_index=True).drop_duplicates()

            # 并行获取历史数据
            hist_df_all = self.fetcher.fetch_hist_data_parallel(codes=list(all_ta_codes), days=120)

            # 一次性计算所有技术指标
            technical_results = self.processor.process_all_technical_indicators(
                all_ta_codes, hist_df_all, ta_source_df)

            macd_df = technical_results['macd_df']
            cci_df = technical_results['cci_df']
            rsi_df = technical_results['rsi_df']
            adx_df = technical_results['adx_df']
            boll_df = technical_results['boll_df']

            # --- 最终推荐和报告生成 ---
            # 【修改点 E: 传入 filtered_spot 用于获取动态市盈率】
            recommended_stocks = self.processor.find_recommended_stocks_with_score(
                macd_df, cci_df, processed_xstp_df, rsi_df, processed_strong_stocks,
                filtered_spot, processed_consecutive_rise,
                adx_df, boll_df, processed_ljqs
            )

            sheets_data = {
                '主力研报筛选': main_report_sheet,
                # '财务摘要数据': processed_financial_abstract, # PEG计算逻辑已移除
                '实时行情': filtered_spot,
                '行业板块': industry_board_df,  # 行业板块名称数据不涉及清洗
                '市场资金流向': processed_market_fund_flow,
                '前十板块成分股': top_industry_cons_df,  # 板块成分股数据不涉及清洗
                '均线多头排列': processed_xstp_df,
                '向上突破': processed_xstp_df,
                '强势股池': processed_strong_stocks,
                '连续上涨': processed_consecutive_rise,
                'MACD金叉': macd_df,
                'CCI超卖': cci_df,
                'RSI金叉': rsi_df,
                'ADX强势': adx_df,
                'BOLL低波': boll_df,
                # 新增工作表
                '量价齐升': processed_ljqs,
                '指标汇总': recommended_stocks,
            }
            self.reporter.generate_report(sheets_data)
        except Exception as e:
            print(f"[FATAL] 致命错误：数据分析流程意外终止。原因: {e}")
            self.reporter.cleanup()
            raise
        finally:
            end_time = time.time()
            print(f"\n>>> 流程结束。总耗时: {end_time - start_time:.2f} 秒。")


# ==============================================================================
# 程序入口
# ==============================================================================
if __name__ == '__main__':
    pipeline = StockDataPipeline()
    pipeline.run()
