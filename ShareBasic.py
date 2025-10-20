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

    def get_file_path(self, base_name: str) -> str:
        """根据基础文件名和当前日期生成完整的文件路径。"""
        file_name = f"{base_name}_{self.today_str}.txt"
        return os.path.join(self.config.TEMP_DATA_DIRECTORY, file_name)

    def load_data_from_txt(self, file_path: str) -> pd.DataFrame:
        """从 | 分隔的 TXT 文件加载数据。"""
        if os.path.exists(file_path):
            try:
                print(f"发现临时文件: {os.path.basename(file_path)}，直接加载数据。")
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
        """带缓存和重试功能的数据获取函数。"""
        file_path = self.get_file_path(file_base_name)
        cached_df = self.load_data_from_txt(file_path)
        if not cached_df.empty:
            return cached_df
        for i in range(self.config.DATA_FETCH_RETRIES):
            try:
                print(f"正在尝试第 {i + 1}/{self.config.DATA_FETCH_RETRIES} 次获取数据: {file_base_name}...")
                df = fetch_func(**kwargs)
                if df is not None and not df.empty:
                    print("数据获取成功。")
                    self.save_data_to_txt(df, file_path)
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
        for _, row in top_industries.iterrows():
            industry_name = row['板块名称']
            print(f"  - 正在获取板块 '{industry_name}' 的成分股...")
            file_base_name = f"板块成分股_{industry_name}"
            constituents_df = self.fetch_with_cache(ak.stock_board_industry_cons_em, file_base_name,
                                                    symbol=industry_name)
            if not constituents_df.empty:
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

        if all_constituents:
            merged_df = pd.concat(all_constituents, ignore_index=True)
            print(f"  - 已合并所有前板块成分股数据，共 {len(merged_df)} 条。")
            return merged_df
        else:
            print("  - 所有板块成分股数据均获取失败。")
            return pd.DataFrame()

    def fetch_hist_data_parallel(self, codes: list, days: int) -> pd.DataFrame:
        """并行获取指定股票代码的历史数据，并缓存到本地文件。"""
        print(f"\n正在为 {len(codes)} 只股票下载 {days} 天的历史数据，使用5个线程并行处理。")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        if os.path.exists(self.macd_cache_file):
            print(f"发现历史数据缓存文件，直接加载。")
            return pd.read_csv(self.macd_cache_file, sep='|', encoding='utf-8', dtype={'股票代码': str})
        all_data = []
        with ThreadPoolExecutor(max_workers=15) as executor:
            future_to_code = {
                executor.submit(
                    ak.stock_zh_a_hist_tx,
                    symbol=format_stock_code(code),
                    start_date=start_date_str,
                    end_date=end_date_str,
                    adjust="hfq"
                ): code
                for code in codes
            }
            for i, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    hist_df = future.result()
                    if hist_df is not None and not hist_df.empty:
                        hist_df['股票代码'] = code
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
            print(f"[WARN] 未能在数据中找到股票代码列，原始列名: {df.columns.tolist()}")
            return pd.DataFrame()
        found_name_col = False
        for old_name, new_name in self.name_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_name_col = True
                break
        if not found_name_col and '股票简称' not in df.columns:
            print(f"[WARN] 未能在数据中找到股票名称列，原始列名: {df.columns.tolist()}")
        found_price_col = False
        for old_name, new_name in self.price_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_price_col = True
                break
        if not found_price_col and '最新价' not in df.columns:
            print(f"[WARN] 未能在数据中找到价格列，原始列名: {df.columns.tolist()}")
        return df

    def clean_data(self, df: pd.DataFrame, df_name: str) -> pd.DataFrame:
        """通用数据清洗函数，处理缺失值、重复值并去除ST股。"""
        initial_rows = len(df)
        df = self.standardize_columns(df)
        if df.empty or '股票代码' not in df.columns:
            print(f"[WARN] {df_name} 数据标准化失败或为空，跳过清洗。")
            return pd.DataFrame()
        df.dropna(subset=['股票代码'], inplace=True)
        df.drop_duplicates(subset=['股票代码'], inplace=True)
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
        if '股票简称' in df.columns:
            cleaned_df = df[~df['股票简称'].str.contains('ST|st|退市', case=False, na=False)].copy()
        else:
            cleaned_df = df.copy()
        final_rows = len(cleaned_df)
        print(f"{df_name} 清洗完成。清洗前：{initial_rows} 条，清洗后：{final_rows} 条。")
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
        final_df = pd.merge(profit_df, spot_df[['股票代码', '最新价']], on='股票代码', how='left')
        final_df['最新价'] = final_df['最新价'].fillna('N/A')
        final_df['股票链接'] = 'https://hybrid.gelonghui.com/stock-check/' + final_df['完整股票编码'].astype(
            str).str.lower()
        final_cols = ['股票代码', '完整股票编码', '股票链接', '最新价']
        other_cols = [col for col in final_df.columns if col not in final_cols]
        return final_df[final_cols + other_cols]

    def process_spot_data(self, spot_data_all: pd.DataFrame, filtered_codes_df: pd.DataFrame) -> pd.DataFrame:
        """处理实时行情数据，并确保价格列名为'当前价格'。"""
        if spot_data_all.empty or filtered_codes_df.empty:
            return pd.DataFrame()
        spot_data_all = self.clean_data(spot_data_all, "A股实时行情")
        if spot_data_all.empty:
            return pd.DataFrame()
        # 核心修复：确保价格列名为'当前价格'
        if '最新价' in spot_data_all.columns:
            spot_data_all.rename(columns={'最新价': '当前价格'}, inplace=True)
        elif '现价' in spot_data_all.columns:
            spot_data_all.rename(columns={'现价': '当前价格'}, inplace=True)
        # 确保合并后保留'股票代码'和'当前价格'
        filtered_spot_data = pd.merge(spot_data_all, filtered_codes_df[['股票代码', '完整股票编码']], on='股票代码',
                                      how='inner')
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
        return self.clean_data(df, name)

    def process_board_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """通用板块数据处理，进行清洗和标准化。"""
        return self.clean_data(df, "板块数据")

    def process_and_merge_xstp_data(self, df20: pd.DataFrame, df60: pd.DataFrame, df90: pd.DataFrame,
                                    spot_data_all: pd.DataFrame) -> pd.DataFrame:
        """处理并合并20日、60日和90日均线数据，并添加实时价格过滤。"""
        print("正在处理并合并20日、60日和90日均线数据...")
        processed_df20 = self.process_board_data(df20.rename(columns={'最新价': '20日均线最新价'}))
        processed_df60 = self.process_board_data(df60.rename(columns={'最新价': '60日均线最新价'}))
        processed_df90 = self.process_board_data(df90.rename(columns={'最新价': '90日均线最新价'}))
        if processed_df20.empty and processed_df60.empty and processed_df90.empty:
            print("[WARN] 所有均线数据均为空，无法合并。")
            return pd.DataFrame()
        merged_df = processed_df20[['股票代码', '股票简称', '20日均线最新价']].copy()
        merged_df = pd.merge(merged_df, processed_df60[['股票代码', '股票简称', '60日均线最新价']], on='股票代码',
                             how='outer', suffixes=('_x', '_y'))
        merged_df['股票简称'] = merged_df['股票简称_x'].fillna(merged_df['股票简称_y'])
        merged_df.drop(columns=['股票简称_x', '股票简称_y'], inplace=True)
        merged_df = pd.merge(merged_df, processed_df90[['股票代码', '股票简称', '90日均线最新价']], on='股票代码',
                             how='outer', suffixes=('_x', '_y'))
        merged_df['股票简称'] = merged_df['股票简称_x'].fillna(merged_df['股票简称_y'])
        merged_df.drop(columns=['股票简称_x', '股票简称_y'], inplace=True)
        final_cols = ['股票代码', '股票简称', '20日均线最新价', '60日均线最新价', '90日均线最新价']
        final_merged_df = merged_df[[col for col in final_cols if col in merged_df.columns]].copy()
        print("正在将实时价格合并到均线数据集中...")
        # 确保用于合并的spot_data_all DataFrame包含正确的列名
        spot_data_all.rename(columns={'最新价': '当前价格'}, inplace=True)
        final_merged_df = pd.merge(final_merged_df, spot_data_all[['股票代码', '当前价格']], on='股票代码', how='left')
        final_merged_df['20日均线最新价'] = pd.to_numeric(final_merged_df['20日均线最新价'], errors='coerce')
        final_merged_df['60日均线最新价'] = pd.to_numeric(final_merged_df['60日均线最新价'], errors='coerce')
        final_merged_df['90日均线最新价'] = pd.to_numeric(final_merged_df['90日均线最新价'], errors='coerce')
        final_merged_df['当前价格'] = pd.to_numeric(final_merged_df['当前价格'], errors='coerce')
        # 集中执行所有过滤条件
        filtered_df = final_merged_df[
            (final_merged_df['股票简称'].notna()) &
            (final_merged_df['20日均线最新价'].notna()) &
            (final_merged_df['当前价格'].notna()) &
            (final_merged_df['当前价格'] > final_merged_df['20日均线最新价'])
            ].copy()
        # 添加多头排列条件
        filtered_df = filtered_df[
            (filtered_df['20日均线最新价'] > filtered_df['60日均线最新价']) |
            (filtered_df['60日均线最新价'] > filtered_df['90日均线最新价'])
            ].copy()
        filtered_df.fillna('N/A', inplace=True)
        print("均线数据合并与过滤完成。")
        return filtered_df

    def process_all_technical_indicators(self, all_ta_codes: list, hist_df_all: pd.DataFrame,
                                         source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """集中处理所有技术指标，避免重复计算。"""
        print(f"\n正在对 {len(all_ta_codes)} 只股票进行批量技术分析...")
        results = {'macd': [], 'cci': [], 'rsi': []}
        grouped = hist_df_all.groupby('股票代码')
        for code, group_df in grouped:
            try:
                if len(group_df) < 30:
                    continue
                group_df.rename(columns={'收盘': 'close'}, inplace=True)
                group_df.ta.macd(close=group_df['close'], append=True)
                if len(group_df) >= 2:
                    last_day_macd = group_df.iloc[-1]
                    prev_day_macd = group_df.iloc[-2]
                    is_golden_cross = (prev_day_macd['MACD_12_26_9'] < prev_day_macd['MACDs_12_26_9']) and (
                            last_day_macd['MACD_12_26_9'] > last_day_macd['MACDs_12_26_9'])
                    if is_golden_cross:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['macd'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'MACD (DIF)': f"{last_day_macd['MACD_12_26_9']:.2f}",
                            'MACD信号线 (DEA)': f"{last_day_macd['MACDs_12_26_9']:.2f}",
                            'MACD动能柱': f"{last_day_macd['MACDh_12_26_9']:.2f}",
                            'MACD买卖信号': '金叉 (买入信号)',
                        })
                group_df.rename(columns={'最高': 'high', '最低': 'low'}, inplace=True)
                group_df.ta.cci(append=True)
                if len(group_df) >= 2:
                    last_day_cci = group_df.iloc[-1]
                    prev_day_cci = group_df.iloc[-2]
                    is_oversold_signal = prev_day_cci['CCI_14_0.015'] < -100 and last_day_cci['CCI_14_0.015'] > -100
                    if is_oversold_signal:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['cci'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新CCI值': f"{last_day_cci['CCI_14_0.015']:.2f}",
                            'CCI买卖信号': '超卖买入信号',
                        })
                group_df.ta.rsi(length=6, append=True)
                group_df.ta.rsi(length=14, append=True)
                if len(group_df) >= 2 and 'RSI_6' in group_df.columns and 'RSI_14' in group_df.columns:
                    last_day_rsi = group_df.iloc[-1]
                    prev_day_rsi = group_df.iloc[-2]
                    is_golden_cross_rsi = (prev_day_rsi['RSI_6'] < prev_day_rsi['RSI_14']) and (
                            last_day_rsi['RSI_6'] > last_day_rsi['RSI_14'])
                    is_rsi14_in_range = 60 <= last_day_rsi['RSI_14'] <= 80
                    if is_golden_cross_rsi and is_rsi14_in_range:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['rsi'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'RSI6': round(last_day_rsi['RSI_6'], 2),
                            'RSI14': round(last_day_rsi['RSI_14'], 2),
                            'RSI买卖信号': '金叉 (买入信号)',
                        })
            except Exception as e:
                print(f"[ERROR] 错误：计算 {code} 的技术指标时出错: {e}，已跳过。")
        macd_df = pd.DataFrame(results['macd']) if results['macd'] else pd.DataFrame()
        cci_df = pd.DataFrame(results['cci']) if results['cci'] else pd.DataFrame()
        rsi_df = pd.DataFrame(results['rsi']) if results['rsi'] else pd.DataFrame()
        print(
            f"MACD金叉: {len(macd_df)} 只，CCI超卖: {len(cci_df)} 只，RSI金叉: {len(rsi_df)} 只。")
        return {'macd_df': macd_df, 'cci_df': cci_df, 'rsi_df': rsi_df, }

    def find_recommended_stocks_with_score(self, macd_df: pd.DataFrame, cci_df: pd.DataFrame, xstp_df: pd.DataFrame,
                                           rsi_df: pd.DataFrame, strong_stocks_df: pd.DataFrame,
                                           filtered_spot: pd.DataFrame,
                                           consecutive_rise_df: pd.DataFrame) -> pd.DataFrame:
        """基于多因子评分筛选推荐股票。"""
        print("\n正在基于多因子评分筛选推荐股票...")

        # 修复开始：筛选非空且包含所需列的 DataFrame
        input_dfs = [macd_df, cci_df, xstp_df, rsi_df]
        df_to_concat = []

        for df in input_dfs:
            if not df.empty and '股票代码' in df.columns and '股票简称' in df.columns:
                df_to_concat.append(df[['股票代码', '股票简称']])

        if not df_to_concat:
            print("[WARN] 未找到任何符合任一条件的股票。")
            # 返回一个具有预期列名的空 DataFrame，以便后续报告生成不报错
            return pd.DataFrame(columns=['股票代码', '股票简称', '符合条件数量', 'MACD买卖信号',
                                         'CCI买卖信号', 'RSI买卖信号', '均线多头排列',
                                         '强势股池', '连涨天数', '当前价格', '股票链接'])

        all_codes = pd.concat(df_to_concat, ignore_index=True).drop_duplicates()
        # 修复结束

        if all_codes.empty:
            print("[WARN] 未找到任何符合任一条件的股票。")
            return pd.DataFrame()
        # 增加对股票简称的过滤
        all_codes = all_codes[~all_codes['股票简称'].str.contains('ST|st|退市', case=False, na=False)].copy()
        final_df = all_codes.copy()
        final_df['符合条件数量'] = 0
        final_df['MACD买卖信号'] = '未满足'
        final_df['CCI买卖信号'] = '未满足'
        final_df['RSI买卖信号'] = '未满足'
        final_df['均线多头排列'] = '未满足'

        def update_df(source_df: pd.DataFrame, column_name: str, check_col: str = None):
            # 确保 source_df 包含 '股票代码' 和 '股票简称'
            if '股票代码' not in source_df.columns:
                print(f"[WARN] 传入 update_df 的 DataFrame 缺少 '股票代码' 列，跳过更新。")
                return

            for _, row in source_df.iterrows():
                code = row['股票代码']
                if code in final_df['股票代码'].values:
                    # 使用 loc 进行安全更新
                    final_df.loc[final_df['股票代码'] == code, '符合条件数量'] += 1
                    if check_col:
                        final_df.loc[final_df['股票代码'] == code, column_name] = row[check_col]
                    else:
                        final_df.loc[final_df['股票代码'] == code, column_name] = '已满足'

        update_df(macd_df, 'MACD买卖信号', 'MACD买卖信号')
        update_df(cci_df, 'CCI买卖信号', 'CCI买卖信号')
        update_df(rsi_df, 'RSI买卖信号', 'RSI买卖信号')
        update_df(xstp_df, '均线多头排列')
        strong_stocks_codes = set(strong_stocks_df['股票代码'])
        final_df['强势股池'] = final_df['股票代码'].apply(lambda x: 'YES' if x in strong_stocks_codes else 'NO')

        # 新增：合并连涨天数数据
        if not consecutive_rise_df.empty and '连涨天数' in consecutive_rise_df.columns:
            consecutive_rise_df['连涨天数'] = pd.to_numeric(consecutive_rise_df['连涨天数'], errors='coerce')
            final_df = pd.merge(final_df, consecutive_rise_df[['股票代码', '连涨天数']], on='股票代码', how='left')
            final_df['连涨天数'] = final_df['连涨天数'].fillna(0).astype(int)
        else:
            final_df['连涨天数'] = 0

        # 合并最新价，使用filtered_spot中的 '当前价格' 列
        final_df = pd.merge(final_df, filtered_spot[['股票代码', '当前价格']], on='股票代码', how='left')
        final_df['当前价格'] = final_df['当前价格'].fillna('N/A')
        final_df['股票链接'] = final_df['股票代码'].apply(
            lambda x: f'https://hybrid.gelonghui.com/stock-check/sh{x}' if str(x).startswith(
                '6') else f'https://hybrid.gelonghui.com/stock-check/sz{x}')
        recommended_df = final_df[final_df['符合条件数量'] >= 1].sort_values(by='符合条件数量',
                                                                             ascending=False).reset_index(
            drop=True)
        recommended_df.fillna('N/A', inplace=True)
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
        self.writer = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
        self.workbook = self.writer.book
        self.header_format = self.workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
        self.text_format = self.workbook.add_format({'border': 1})
        self.link_format = self.workbook.add_format({'border': 1, 'font_color': 'blue', 'underline': 1})
        self.red_format = self.workbook.add_format({'border': 1, 'font_color': 'red'})
        self.green_format = self.workbook.add_format({'border': 1, 'font_color': 'green'})

    def _write_dataframe(self, df: pd.DataFrame, sheet_name: str, link_col: str = None,
                         conditional_format: Dict[str, Any] = None):
        """通用写入DataFrame到Excel的方法。"""
        if df.empty:
            print(f"[WARN] {sheet_name} 数据为空，跳过生成该工作表。")
            return
        try:
            worksheet = self.workbook.add_worksheet(sheet_name)
            df.to_excel(self.writer, sheet_name=sheet_name, index=False, startrow=1, header=False)
            for col_num, value in enumerate(df.columns):
                worksheet.write(0, col_num, value, self.header_format)
                worksheet.set_column(col_num, col_num, 15, self.text_format)
            if link_col and link_col in df.columns:
                link_col_idx = df.columns.get_loc(link_col)
                for row_num, link in enumerate(df[link_col], 1):
                    try:
                        if link and link != 'N/A':
                            worksheet.write_url(row_num, link_col_idx, link, self.link_format, '点击链接')
                        else:
                            worksheet.write(row_num, link_col_idx, 'N/A', self.text_format)
                    except xlsxwriter.exceptions.XlsxWriterException:
                        worksheet.write(row_num, link_col_idx, '链接无效', self.text_format)
            if conditional_format:
                for condition in conditional_format:
                    col_name = condition['column']
                    if col_name in df.columns:
                        col_idx = df.columns.get_loc(col_name)
                        for row_num, value in enumerate(df[col_name], 1):
                            if condition['check'](value):
                                worksheet.write(row_num, col_idx, value, condition['format'])
        except Exception as e:
            print(f"[ERROR] 写入工作表 {sheet_name} 时出错: {e}")

    def generate_report(self, sheets_data: Dict[str, pd.DataFrame]):
        """生成最终的Excel报告。"""
        print("\n>>> 正在生成Excel报告...")
        sheet_specs = {
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
            '指标汇总': {'df': sheets_data.get('指标汇总'), 'link_col': '股票链接', 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format},
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]}}
        try:
            for sheet_name, spec in sheet_specs.items():
                if spec['df'] is not None and not spec['df'].empty:
                    self._write_dataframe(spec['df'], sheet_name, spec.get('link_col'), spec.get('conditional_format'))
            print(f"报告已成功生成: {self.file_path}")
        except Exception as e:
            print(f"[ERROR] 生成Excel报告时出错: {e}")
            raise
        finally:
            if self.writer:
                self.writer.close()

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
            profit_data_raw = self.fetcher.fetch_with_cache(ak.stock_profit_forecast_em, '主力研报盈利预测')
            spot_data_all = self.fetcher.fetch_with_cache(ak.stock_zh_a_spot_em, 'A股实时行情')
            market_fund_flow_raw = self.fetcher.fetch_with_cache(ak.stock_fund_flow_individual, '市场资金流向',
                                                                 symbol="5日排行")
            industry_board_df = self.fetcher.fetch_with_cache(ak.stock_board_industry_name_em, '行业板块名称')
            financial_abstract_df = self.fetcher.fetch_with_cache(ak.stock_financial_abstract, '财务摘要数据')
            strong_stocks_df_raw = self.fetcher.fetch_with_cache(ak.stock_rank_ljqd_ths, '强势股池')
            consecutive_rise_df_raw = self.fetcher.fetch_with_cache(ak.stock_rank_lxsz_ths, '连续上涨')
            df_ma20 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破20日均线', symbol="20日均线")
            df_ma60 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破60日均线', symbol="60日均线")
            df_ma90 = self.fetcher.fetch_with_cache(ak.stock_rank_xstp_ths, '向上突破90日均线', symbol="90日均线")
            print("\n>>> 正在进行数据处理和筛选...")
            processed_profit_data = self.processor.process_profit_data(profit_data_raw)
            processed_spot_data = self.processor.clean_data(spot_data_all, "A股实时行情")
            main_report_sheet = self.processor.process_main_report_sheet(processed_profit_data, processed_spot_data)
            filtered_spot = self.processor.process_spot_data(processed_spot_data, processed_profit_data)
            processed_financial_abstract = self.processor.process_financial_abstract(financial_abstract_df)
            processed_market_fund_flow = self.processor.process_market_fund_flow(market_fund_flow_raw)
            processed_strong_stocks = self.processor.process_general_rank(strong_stocks_df_raw, '强势股池')
            processed_consecutive_rise = self.processor.process_general_rank(consecutive_rise_df_raw, '连续上涨')
            # 使用新的获取方法
            top_industry_cons_df = self.fetcher.get_top_industry_stocks()
            processed_xstp_df = self.processor.process_and_merge_xstp_data(df_ma20, df_ma60, df_ma90,
                                                                           processed_spot_data)
            # 合并所有需要进行技术分析的股票代码，只获取一次历史数据
            all_ta_codes = set(main_report_sheet['股票代码'].tolist()) | set(df_ma20['股票代码'].tolist())
            all_ta_codes = [code for code in all_ta_codes if pd.notna(code)]
            # 并行获取历史数据
            hist_df_all = self.fetcher.fetch_hist_data_parallel(codes=list(all_ta_codes), days=120)
            # 一次性计算所有技术指标
            technical_results = self.processor.process_all_technical_indicators(
                all_ta_codes, hist_df_all, pd.concat([main_report_sheet, df_ma20]))
            macd_df = technical_results['macd_df']
            cci_df = technical_results['cci_df']
            rsi_df = technical_results['rsi_df']
            # 基于多因子评分筛选推荐股票
            recommended_stocks = self.processor.find_recommended_stocks_with_score(
                macd_df, cci_df, processed_xstp_df, rsi_df, processed_strong_stocks,
                filtered_spot, processed_consecutive_rise)  # 新增的参数
            sheets_data = {
                '主力研报筛选': main_report_sheet,
                '财务摘要数据': processed_financial_abstract,
                '实时行情': filtered_spot,
                '行业板块': industry_board_df,
                '市场资金流向': processed_market_fund_flow,
                '前十板块成分股': top_industry_cons_df,
                '均线多头排列': processed_xstp_df,
                '向上突破': processed_xstp_df,
                '强势股池': processed_strong_stocks,
                '连续上涨': processed_consecutive_rise,
                'MACD金叉': macd_df,
                'CCI超卖': cci_df,
                'RSI金叉': rsi_df,
                '指标汇总': recommended_stocks,
            }
            self.reporter.generate_report(sheets_data)
        except Exception as e:
            print(f"[FATAL] 致命错误：数据分析流程意外终止。原因: {e}")
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
