import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from typing import Callable, Dict, Any, List
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

    def get_file_path(self, base_name: str) -> str:
        """根据基础文件名和当前日期生成完整的文件路径。"""
        file_name = f"{base_name}_{self.today_str}.txt"
        return os.path.join(self.config.TEMP_DATA_DIRECTORY, file_name)

    def load_data_from_txt(self, file_path: str) -> pd.DataFrame:
        """从 | 分隔的 TXT 文件加载数据。"""
        if os.path.exists(file_path):
            try:
                print(f"发现临时文件: {os.path.basename(file_path)}，直接加载数据。")
                # 显式指定 '成交量' 为数值类型，'股票代码' 为字符串
                df = pd.read_csv(file_path, sep='|', encoding='utf-8',
                                 dtype={'股票代码': str},
                                 converters={'成交量': lambda x: pd.to_numeric(x, errors='coerce')})
                return df
            except Exception as e:
                print(f"[WARN] 错误：加载临时文件 {os.path.basename(file_path)} 失败: {e}，将重新获取。")
        return pd.DataFrame()

    def save_data_to_txt(self, df: pd.DataFrame, file_path: str):
        """将 DataFrame 保存到 | 分隔的 TXT 文件。"""
        try:
            # 在保存前，将包含 NaN 的数字列转换为 float，确保能被 xlsxwriter 正确处理
            for col in df.select_dtypes(include=[np.number]).columns:
                if df[col].isnull().any():
                    df[col] = df[col].astype(float)

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
        # 1. 获取行业板块名称列表 (使用缓存)
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
            # 2. 获取板块成分股 (使用缓存)
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

    # 重构：使用 fetch_with_cache 实现带日期的缓存
    def fetch_hist_data_parallel(self, codes: List[str], days: int) -> pd.DataFrame:
        """
        并行获取指定股票代码的历史数据，并缓存到带日期的本地文件。
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # 核心并行下载逻辑，作为 fetch_with_cache 的 Callable 参数
        def _fetch_all_hist_data(codes, start_date_str, end_date_str):
            print(f"正在为 {len(codes)} 只股票下载 {days} 天的历史数据，使用10个线程并行处理。")
            all_data = []
            with ThreadPoolExecutor(max_workers=10) as executor:
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
                            # 确保成交量是数字类型
                            if '成交量' in hist_df.columns:
                                hist_df['成交量'] = pd.to_numeric(hist_df['成交量'], errors='coerce')
                            all_data.append(hist_df)
                            # print(f"成功下载 {code} 的历史数据 ({i + 1}/{len(codes)})")
                        if (i + 1) % 50 == 0:
                            print(f"  - 已完成 {i + 1}/{len(codes)} 只股票的历史数据下载。")
                    except Exception as e:
                        print(f"[ERROR] 错误：获取 {code} 的历史数据时出错: {e}，已跳过。")
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            return pd.DataFrame()

        file_base_name = f"AllHistData_D{days}"
        return self.fetch_with_cache(
            fetch_func=_fetch_all_hist_data,
            file_base_name=file_base_name,
            codes=codes,
            start_date_str=start_date_str,
            end_date_str=end_date_str
        )


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

        # 明确定义技术指标参数
        self.TA_PARAMS = {
            'MACD': {'fast': 12, 'slow': 26, 'signal': 9},
            'CCI': {'length': 14, 'c': 0.015},
            'RSI': {'short': 6, 'long': 14},
            'OBV': {'MA_SHORT': 5, 'MA_LONG': 20}
        }

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 的列名，确保'股票代码'和'股票简称'等列存在。"""
        if df.empty:
            return df
        # 简化列名标准化逻辑
        column_mapping = {}
        for old_name, new_name in self.code_aliases.items():
            if old_name in df.columns:
                column_mapping[old_name] = new_name
        for old_name, new_name in self.name_aliases.items():
            if old_name in df.columns:
                column_mapping[old_name] = new_name
        for old_name, new_name in self.price_aliases.items():
            if old_name in df.columns:
                column_mapping[old_name] = new_name

        if column_mapping:
            df.rename(columns=column_mapping, inplace=True)

        if '股票代码' not in df.columns:
            print(f"[WARN] 未能在数据中找到股票代码列，原始列名: {df.columns.tolist()}")
            return pd.DataFrame()
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
        # 过滤掉评级低于 min_rating 的股票
        df = df[df['机构投资评级(近六个月)-买入'] >= min_rating].copy()
        df['完整股票编码'] = df['股票代码'].apply(format_stock_code)
        print(f"研报数据过滤完成，符合条件的股票数量: {len(df)}")
        return df

    def process_main_report_sheet(self, profit_df: pd.DataFrame, spot_df: pd.DataFrame) -> pd.DataFrame:
        """生成“主力研报筛选” Sheet 的数据。"""
        if profit_df.empty or spot_df.empty:
            print("[WARN] 研报数据或实时行情数据为空，无法生成主力研报筛选表。")
            return pd.DataFrame()

        # 确保 spot_df 中有 '最新价' 列
        temp_spot_df = self.standardize_columns(spot_df[['股票代码', '最新价']].copy())

        final_df = pd.merge(profit_df, temp_spot_df, on='股票代码', how='left')
        final_df['最新价'] = final_df['最新价'].fillna('N/A')

        # 优化股票链接生成
        final_df['股票链接'] = final_df['完整股票编码'].apply(
            lambda x: f'https://hybrid.gelonghui.com/stock-check/{str(x).lower()}'
        )

        final_cols = ['股票代码', '股票简称', '完整股票编码', '股票链接', '最新价']
        other_cols = [col for col in final_df.columns if
                      col not in final_cols and col not in self.code_aliases.values() and col not in self.name_aliases.values()]
        return final_df[final_cols + other_cols]

    def process_spot_data(self, spot_data_all: pd.DataFrame, filtered_codes_df: pd.DataFrame) -> pd.DataFrame:
        """处理实时行情数据，并确保价格列名为'当前价格'。"""
        if spot_data_all.empty or filtered_codes_df.empty:
            return pd.DataFrame()

        spot_data_all = self.clean_data(spot_data_all, "A股实时行情")
        if spot_data_all.empty:
            return pd.DataFrame()

        # 确保价格列名为'当前价格'
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
        """处理市场资金流向数据 (5日排行)。"""
        df = self.clean_data(df, "市场资金流向")
        if df.empty:
            print("警告：未能获取市场资金流向数据。")
            return pd.DataFrame()

        if '流入资金' in df.columns:
            df['流入资金'] = pd.to_numeric(df['流入资金'], errors='coerce')
            df = df.sort_values(by='流入资金', ascending=False).copy()
        else:
            print("警告：未能找到 '流入资金' 列进行排序。")
        print(f"  - 市场资金流向数据处理成功，共 {len(df)} 条。")
        return df

    def process_general_rank(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """通用排行榜数据处理，添加股票代码和编码。"""
        df = self.clean_data(df, name)
        if not df.empty and '股票代码' in df.columns:
            df['完整股票编码'] = df['股票代码'].apply(format_stock_code)
        return df

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

        # 使用 reduce 或 series merge 进行高效合并
        dfs = [processed_df20[['股票代码', '股票简称', '20日均线最新价']],
               processed_df60[['股票代码', '股票简称', '60日均线最新价']],
               processed_df90[['股票代码', '股票简称', '90日均线最新价']]]

        from functools import reduce
        merged_df = reduce(lambda left, right: pd.merge(left, right, on='股票代码', how='outer', suffixes=('', '_y')),
                           dfs)

        # 修复合并后股票简称列可能出现的冗余
        if '股票简称_y' in merged_df.columns:
            merged_df['股票简称'] = merged_df['股票简称'].fillna(merged_df['股票简称_y'])
            merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_y')], inplace=True)

        final_cols = ['股票代码', '股票简称', '20日均线最新价', '60日均线最新价', '90日均线最新价']
        final_merged_df = merged_df[[col for col in final_cols if col in merged_df.columns]].copy()

        print("正在将实时价格合并到均线数据集中...")
        # 确保用于合并的spot_data_all DataFrame包含正确的列名
        spot_data_all = self.standardize_columns(spot_data_all.copy())
        if '最新价' in spot_data_all.columns:
            spot_data_all.rename(columns={'最新价': '当前价格'}, inplace=True)

        final_merged_df = pd.merge(final_merged_df, spot_data_all[['股票代码', '当前价格']], on='股票代码', how='left')

        # 转换为数值类型
        for col in ['20日均线最新价', '60日均线最新价', '90日均线最新价', '当前价格']:
            final_merged_df[col] = pd.to_numeric(final_merged_df[col], errors='coerce')

        # 集中执行过滤条件
        # 1. 向上突破 20 日均线 (当前价格 > 20日均线)
        # 2. 严格的多头排列 (20日均线 > 60日均线 > 90日均线)
        filtered_df = final_merged_df[
            (final_merged_df['股票简称'].notna()) &
            (final_merged_df['当前价格'].notna()) &
            (final_merged_df['20日均线最新价'].notna()) &
            (final_merged_df['60日均线最新价'].notna()) &
            (final_merged_df['90日均线最新价'].notna()) &
            (final_merged_df['当前价格'] > final_merged_df['20日均线最新价']) &  # 向上突破 20 日线
            (final_merged_df['20日均线最新价'] > final_merged_df['60日均线最新价']) &  # 严格多头排列
            (final_merged_df['60日均线最新价'] > final_merged_df['90日均线最新价'])  # 严格多头排列
            ].copy()

        filtered_df.fillna('N/A', inplace=True)
        print("均线数据合并与过滤完成。")
        return filtered_df

    def process_all_technical_indicators(self, all_ta_codes: list, hist_df_all: pd.DataFrame,
                                         source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """集中处理所有技术指标，避免重复计算。"""
        print(f"\n正在对 {len(all_ta_codes)} 只股票进行批量技术分析...")
        results = {'macd': [], 'cci': [], 'rsi': [], 'obv': []}
        grouped = hist_df_all.groupby('股票代码')

        # 统一列名以适应 pandas_ta
        column_map = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'}

        # 获取参数
        macd_p = self.TA_PARAMS['MACD']
        cci_p = self.TA_PARAMS['CCI']
        rsi_p = self.TA_PARAMS['RSI']
        obv_p = self.TA_PARAMS['OBV']

        # 构造列名
        macd_dif_col = f"MACD_{macd_p['fast']}_{macd_p['slow']}_{macd_p['signal']}"
        macd_dea_col = f"MACDs_{macd_p['fast']}_{macd_p['slow']}_{macd_p['signal']}"
        macd_hist_col = f"MACDh_{macd_p['fast']}_{macd_p['slow']}_{macd_p['signal']}"
        cci_col = f"CCI_{cci_p['length']}_{cci_p['c']}"
        rsi_short_col = f"RSI_{rsi_p['short']}"
        rsi_long_col = f"RSI_{rsi_p['long']}"

        for code, group_df_raw in grouped:
            try:
                group_df = group_df_raw.copy()
                group_df.rename(columns=column_map, inplace=True)

                for col in ['close', 'high', 'low', 'volume']:
                    group_df[col] = pd.to_numeric(group_df[col], errors='coerce')

                group_df.dropna(subset=['close', 'high', 'low', 'volume'], inplace=True)

                if len(group_df) < 30:  # 至少需要30天数据进行完整计算
                    continue

                # --- 1. MACD ---
                group_df.ta.macd(close=group_df['close'], fast=macd_p['fast'], slow=macd_p['slow'],
                                 signal=macd_p['signal'], append=True)
                if len(group_df) >= 2 and macd_dif_col in group_df.columns:
                    last_day_macd = group_df.iloc[-1]
                    prev_day_macd = group_df.iloc[-2]
                    is_golden_cross = (prev_day_macd[macd_dif_col] < prev_day_macd[macd_dea_col]) and (
                            last_day_macd[macd_dif_col] > last_day_macd[macd_dea_col])
                    if is_golden_cross:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['macd'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'MACD (DIF)': f"{last_day_macd[macd_dif_col]:.2f}",
                            'MACD信号线 (DEA)': f"{last_day_macd[macd_dea_col]:.2f}",
                            'MACD动能柱': f"{last_day_macd[macd_hist_col]:.2f}",
                            'MACD买卖信号': '金叉 (买入信号)',
                        })

                # --- 2. CCI ---
                group_df.ta.cci(close=group_df['close'], high=group_df['high'], low=group_df['low'],
                                length=cci_p['length'], c=cci_p['c'], append=True)
                if len(group_df) >= 2 and cci_col in group_df.columns:
                    last_day_cci = group_df.iloc[-1]
                    prev_day_cci = group_df.iloc[-2]
                    # CCI 从低于 -100 上穿 -100 视为超卖买入信号
                    is_oversold_signal = prev_day_cci[cci_col] < -100 and last_day_cci[cci_col] > -100
                    if is_oversold_signal:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['cci'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新CCI值': f"{last_day_cci[cci_col]:.2f}",
                            'CCI买卖信号': '超卖买入信号',
                        })

                # --- 3. RSI ---
                group_df.ta.rsi(length=rsi_p['short'], append=True)
                group_df.ta.rsi(length=rsi_p['long'], append=True)
                if len(group_df) >= 2 and rsi_short_col in group_df.columns and rsi_long_col in group_df.columns:
                    last_day_rsi = group_df.iloc[-1]
                    prev_day_rsi = group_df.iloc[-2]
                    # RSI6 上穿 RSI14
                    is_golden_cross_rsi = (prev_day_rsi[rsi_short_col] < prev_day_rsi[rsi_long_col]) and (
                            last_day_rsi[rsi_short_col] > last_day_rsi[rsi_long_col])
                    # 且 RSI14 在强势区但未超买
                    is_rsi14_in_range = 50 <= last_day_rsi[rsi_long_col] <= 80
                    if is_golden_cross_rsi and is_rsi14_in_range:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['rsi'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'RSI6': round(last_day_rsi[rsi_short_col], 2),
                            'RSI14': round(last_day_rsi[rsi_long_col], 2),
                            'RSI买卖信号': '金叉 (买入信号)',
                        })

                # --- 4. OBV ---
                group_df.ta.obv(close=group_df['close'], volume=group_df['volume'], append=True)

                if 'OBV' in group_df.columns and len(group_df) >= obv_p['MA_LONG']:
                    group_df['OBV_MA5'] = group_df['OBV'].rolling(window=obv_p['MA_SHORT']).mean()
                    group_df['OBV_MA20'] = group_df['OBV'].rolling(window=obv_p['MA_LONG']).mean()

                    last_day = group_df.iloc[-1]
                    prev_day = group_df.iloc[-2]

                    # OBV 短期均线 (MA5) 向上突破 长期均线 (MA20)
                    is_obv_cross = (prev_day['OBV_MA5'] < prev_day['OBV_MA20']) and \
                                   (last_day['OBV_MA5'] > last_day['OBV_MA20'])

                    if is_obv_cross:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0]
                        results['obv'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新OBV值': f"{last_day['OBV']:.0f}",
                            'OBV买卖信号': '均线金叉 (量能增强)',
                        })

            except Exception as e:
                print(f"[ERROR] 错误：计算 {code} 的技术指标时出错: {e}，已跳过。")

        # 整理结果 DataFrame
        macd_df = pd.DataFrame(results['macd']) if results['macd'] else pd.DataFrame()
        cci_df = pd.DataFrame(results['cci']) if results['cci'] else pd.DataFrame()
        rsi_df = pd.DataFrame(results['rsi']) if results['rsi'] else pd.DataFrame()
        obv_df = pd.DataFrame(results['obv']) if results['obv'] else pd.DataFrame()

        print(
            f"MACD金叉: {len(macd_df)} 只，CCI超卖: {len(cci_df)} 只，RSI金叉: {len(rsi_df)} 只，OBV金叉: {len(obv_df)} 只。")

        return {'macd_df': macd_df, 'cci_df': cci_df, 'rsi_df': rsi_df, 'obv_df': obv_df}

    def find_recommended_stocks_with_score(self, macd_df: pd.DataFrame, cci_df: pd.DataFrame, xstp_df: pd.DataFrame,
                                           rsi_df: pd.DataFrame, obv_df: pd.DataFrame,
                                           strong_stocks_df: pd.DataFrame,
                                           filtered_spot: pd.DataFrame,
                                           consecutive_rise_df: pd.DataFrame) -> pd.DataFrame:
        """基于多因子评分筛选推荐股票 (使用向量化合并进行优化)。"""
        print("\n正在基于多因子评分筛选推荐股票...")

        # 合并所有符合条件的股票代码
        all_codes = pd.concat([macd_df[['股票代码', '股票简称']], cci_df[['股票代码', '股票简称']],
                               xstp_df[['股票代码', '股票简称']], rsi_df[['股票代码', '股票简称']],
                               obv_df[['股票代码', '股票简称']]],
                              ignore_index=True).drop_duplicates()

        if all_codes.empty:
            print("[WARN] 未找到任何符合任一条件的股票。")
            return pd.DataFrame()

        all_codes = all_codes[~all_codes['股票简称'].str.contains('ST|st|退市', case=False, na=False)].copy()

        # 初始化结果 DataFrame
        final_df = all_codes.copy()
        final_df['MACD买卖信号'] = '未满足'
        final_df['CCI买卖信号'] = '未满足'
        final_df['RSI买卖信号'] = '未满足'
        final_df['OBV买卖信号'] = '未满足'
        final_df['均线多头排列'] = '未满足'

        # 向量化更新函数
        def merge_and_update(main_df, sub_df, signal_col, condition_col='均线多头排列'):
            if sub_df.empty:
                return main_df

            # 1. 临时合并，获取信号
            temp_df = pd.merge(main_df[['股票代码']], sub_df[['股票代码', signal_col]],
                               on='股票代码', how='left')

            # 2. 批量更新信号列
            main_df[signal_col] = temp_df[signal_col].combine_first(main_df[signal_col])

            # 3. 批量更新计数列 (如果 sub_df 满足条件，则 condition_col 更新为 '已满足')
            if condition_col != signal_col:
                main_df[condition_col] = np.where(temp_df[signal_col].notna(),
                                                  '已满足',
                                                  main_df[condition_col])
            return main_df

        final_df = merge_and_update(final_df, macd_df, 'MACD买卖信号', 'MACD买卖信号')
        final_df = merge_and_update(final_df, cci_df, 'CCI买卖信号', 'CCI买卖信号')
        final_df = merge_and_update(final_df, rsi_df, 'RSI买卖信号', 'RSI买卖信号')
        final_df = merge_and_update(final_df, obv_df, 'OBV买卖信号', 'OBV买卖信号')

        # 均线多头排列/向上突破，只需要知道是否满足
        temp_xstp = xstp_df[['股票代码']].copy()
        temp_xstp['均线多头排列_Signal'] = '已满足'
        final_df = pd.merge(final_df, temp_xstp, on='股票代码', how='left')
        final_df['均线多头排列'] = final_df['均线多头排列_Signal'].combine_first(final_df['均线多头排列'])
        if '均线多头排列_Signal' in final_df.columns:
            final_df.drop(columns=['均线多头排列_Signal'], inplace=True)

        # 统计符合条件数量
        signal_cols = ['MACD买卖信号', 'CCI买卖信号', 'RSI买卖信号', 'OBV买卖信号', '均线多头排列']
        final_df['符合条件数量'] = (final_df[signal_cols] != '未满足').sum(axis=1)

        # 强势股池
        strong_stocks_codes = set(strong_stocks_df['股票代码'])
        final_df['强势股池'] = final_df['股票代码'].apply(lambda x: 'YES' if x in strong_stocks_codes else 'NO')

        # 合并连涨天数数据
        if not consecutive_rise_df.empty and '连涨天数' in consecutive_rise_df.columns:
            consecutive_rise_df['连涨天数'] = pd.to_numeric(consecutive_rise_df['连涨天数'], errors='coerce')
            final_df = pd.merge(final_df, consecutive_rise_df[['股票代码', '连涨天数']], on='股票代码', how='left')
            final_df['连涨天数'] = final_df['连涨天数'].fillna(0).astype(int)
        else:
            final_df['连涨天数'] = 0

        # 合并最新价，使用filtered_spot中的 '当前价格' 列
        final_df = pd.merge(final_df, filtered_spot[['股票代码', '当前价格']], on='股票代码', how='left')
        final_df['当前价格'] = final_df['当前价格'].fillna('N/A')

        # 重新创建股票链接
        final_df['股票链接'] = final_df['股票代码'].apply(
            lambda x: f'https://hybrid.gelonghui.com/stock-check/{format_stock_code(x).lower()}'
        )

        # 筛选至少满足一个条件的股票
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
        self.writer = None
        self.workbook = None

        # 确保目录存在
        os.makedirs(self.config.SAVE_DIRECTORY, exist_ok=True)

    def _initialize_formats(self):
        """初始化Excel格式"""
        self.header_format = self.workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
        self.text_format = self.workbook.add_format({'border': 1})
        self.link_format = self.workbook.add_format({'border': 1, 'font_color': 'blue', 'underline': 1})
        self.red_format = self.workbook.add_format({'border': 1, 'font_color': 'red'})
        self.green_format = self.workbook.add_format({'border': 1, 'font_color': 'green'})

    def _write_links(self, worksheet, df, link_col):
        """处理链接列的写入"""
        if link_col in df.columns:
            link_col_idx = df.columns.get_loc(link_col)
            for row_num, link in enumerate(df[link_col], 1):
                try:
                    display_text = df.iloc[row_num - 1]['股票代码'] if '股票代码' in df.columns else '链接'
                    if link and link != 'N/A' and link.startswith('http'):
                        worksheet.write_url(row_num, link_col_idx, link, self.link_format, display_text)
                    else:
                        worksheet.write(row_num, link_col_idx, 'N/A', self.text_format)
                except xlsxwriter.exceptions.XlsxWriterException:
                    worksheet.write(row_num, link_col_idx, '链接无效', self.text_format)

    def _apply_conditional_formatting(self, worksheet, df, conditional_format):
        """处理条件格式的应用"""
        if conditional_format:
            for condition in conditional_format:
                col_name = condition['column']
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name)
                    for row_num, value in enumerate(df[col_name], 1):
                        display_value = value if pd.notna(value) else 'N/A'
                        if condition['check'](display_value):
                            worksheet.write(row_num, col_idx, display_value, condition['format'])
                        else:
                            # 确保没有命中条件的单元格也被写入默认格式
                            worksheet.write(row_num, col_idx, display_value, self.text_format)

    def _write_dataframe(self, df: pd.DataFrame, sheet_name: str, link_col: str = None,
                         conditional_format: Dict[str, Any] = None):
        """通用写入DataFrame到Excel的方法。"""
        if df.empty:
            print(f"[WARN] {sheet_name} 数据为空，跳过生成该工作表。")
            return

        # 确保数据写入前 NaN 被填充或转换，避免写入错误
        df_for_excel = df.copy()
        df_for_excel = df_for_excel.replace({np.nan: 'N/A', None: 'N/A'})

        try:
            worksheet = self.workbook.add_worksheet(sheet_name)
            # 使用 writer 写入数据，但跳过默认的 header 和 index
            df_for_excel.to_excel(self.writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

            # 写入自定义表头和设置列宽
            for col_num, value in enumerate(df.columns):
                worksheet.write(0, col_num, value, self.header_format)
                col_width = 15
                if '链接' in value or '简称' in value:
                    col_width = 25
                elif '代码' in value:
                    col_width = 12
                worksheet.set_column(col_num, col_num, col_width, self.text_format)

            # 处理链接和条件格式
            if link_col:
                self._write_links(worksheet, df_for_excel, link_col)
            if conditional_format:
                self._apply_conditional_formatting(worksheet, df_for_excel, conditional_format)

        except Exception as e:
            print(f"[ERROR] 写入工作表 {sheet_name} 时出错: {e}")

    def generate_report(self, sheets_data: Dict[str, pd.DataFrame]):
        """生成最终的Excel报告。"""
        print("\n>>> 正在生成Excel报告...")
        self.writer = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
        self.workbook = self.writer.book
        self._initialize_formats()

        sheet_specs = {
            '指标汇总': {'df': sheets_data.get('指标汇总'), 'link_col': '股票链接', 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format},
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'OBV买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            '主力研报筛选': {'df': sheets_data.get('主力研报筛选'), 'link_col': '股票链接', 'conditional_format': None},
            '均线多头排列': {'df': sheets_data.get('均线多头排列'), 'link_col': None, 'conditional_format': None},
            'MACD金叉': {'df': sheets_data.get('MACD金叉'), 'link_col': None, 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            'CCI超卖': {'df': sheets_data.get('CCI超卖'), 'link_col': None, 'conditional_format': [
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format}]},
            'RSI金叉': {'df': sheets_data.get('RSI金叉'), 'link_col': None, 'conditional_format': [
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            'OBV金叉': {'df': sheets_data.get('OBV金叉'), 'link_col': None, 'conditional_format': [
                {'column': 'OBV买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            '实时行情': {'df': sheets_data.get('实时行情'), 'link_col': None, 'conditional_format': None},
            '行业板块': {'df': sheets_data.get('行业板块'), 'link_col': None, 'conditional_format': None},
            '市场资金流向': {'df': sheets_data.get('市场资金流向'), 'link_col': None, 'conditional_format': None},
            '强势股池': {'df': sheets_data.get('强势股池'), 'link_col': None, 'conditional_format': None},
            '连续上涨': {'df': sheets_data.get('连续上涨'), 'link_col': None, 'conditional_format': None},
            '前十板块成分股': {'df': sheets_data.get('前十板块成分股'), 'link_col': None, 'conditional_format': None},
            '财务摘要数据': {'df': sheets_data.get('财务摘要数据'), 'link_col': None, 'conditional_format': None},
        }

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
            # 基础数据获取 (使用缓存)
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
            processed_spot_data = self.processor.clean_data(spot_data_all.copy(), "A股实时行情")
            main_report_sheet = self.processor.process_main_report_sheet(processed_profit_data, processed_spot_data)

            # 使用经过研报过滤的股票代码集进行实时行情过滤
            filtered_spot = self.processor.process_spot_data(processed_spot_data.copy(), processed_profit_data)

            processed_financial_abstract = self.processor.process_financial_abstract(financial_abstract_df)
            processed_market_fund_flow = self.processor.process_market_fund_flow(market_fund_flow_raw)
            processed_strong_stocks = self.processor.process_general_rank(strong_stocks_df_raw, '强势股池')
            processed_consecutive_rise = self.processor.process_general_rank(consecutive_rise_df_raw, '连续上涨')

            top_industry_cons_df = self.fetcher.get_top_industry_stocks()
            processed_xstp_df = self.processor.process_and_merge_xstp_data(df_ma20, df_ma60, df_ma90,
                                                                           processed_spot_data.copy())

            # 合并所有需要进行技术分析的股票代码
            all_ta_codes = set(main_report_sheet['股票代码'].tolist()) | set(processed_xstp_df['股票代码'].tolist())
            all_ta_codes = [code for code in all_ta_codes if pd.notna(code)]

            # 并行获取历史数据 (使用带日期的缓存)
            hist_df_all = self.fetcher.fetch_hist_data_parallel(codes=list(all_ta_codes), days=120)

            # 将需要技术分析的股票信息合并，作为技术指标计算的参考源
            ta_source_df = pd.concat([main_report_sheet[['股票代码', '股票简称']],
                                      processed_xstp_df[['股票代码', '股票简称']]]).drop_duplicates()

            # 一次性计算所有技术指标
            technical_results = self.processor.process_all_technical_indicators(
                all_ta_codes, hist_df_all, ta_source_df)

            macd_df = technical_results['macd_df']
            cci_df = technical_results['cci_df']
            rsi_df = technical_results['rsi_df']
            obv_df = technical_results['obv_df']

            # 基于多因子评分筛选推荐股票
            recommended_stocks = self.processor.find_recommended_stocks_with_score(
                macd_df, cci_df, processed_xstp_df, rsi_df, obv_df,
                processed_strong_stocks, filtered_spot, processed_consecutive_rise)

            sheets_data = {
                '主力研报筛选': main_report_sheet,
                '财务摘要数据': processed_financial_abstract,
                '实时行情': filtered_spot,
                '行业板块': industry_board_df,
                '市场资金流向': processed_market_fund_flow,
                '前十板块成分股': top_industry_cons_df,
                '均线多头排列': processed_xstp_df,  # 仅保留严格多头排列的股票
                'MACD金叉': macd_df,
                'CCI超卖': cci_df,
                'RSI金叉': rsi_df,
                'OBV金叉': obv_df,
                '强势股池': processed_strong_stocks,
                '连续上涨': processed_consecutive_rise,
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
