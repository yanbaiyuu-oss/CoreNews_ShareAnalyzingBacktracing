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
import backtrader as bt

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


# FIX 1: 修正 backtrader.analyzer -> backtrader.analyzers
def get_trade_metrics(analyzer: bt.analyzers.TradeAnalyzer) -> Dict[str, Any]:
    """从 TradeAnalyzer 中提取交易细节指标，包括平均持有期。"""
    ta = analyzer.get_analysis()

    # 1. Total Metrics
    total_trades = ta.total.closed if 'closed' in ta.total else 0

    # 2. Winning/Losing Trades
    won = ta.won.total if 'won' in ta else 0
    lost = ta.lost.total if 'lost' in ta else 0

    winrate = (won / total_trades) * 100 if total_trades > 0 else 0

    # 3. Profit Factor (Total Gross Profit / Total Gross Loss)
    # PnL.gross.total is total gross profit/loss, PnL.gross.neg.total is total gross loss (absolute value)
    gross_profit = ta.pnl.gross.total if 'pnl' in ta and 'gross' in ta.pnl and 'total' in ta.pnl.gross else 0
    gross_loss_abs = abs(
        ta.pnl.gross.neg.total) if 'pnl' in ta and 'gross' in ta.pnl and 'neg' in ta.pnl and 'total' in ta.pnl.gross.neg else 0

    profit_factor = abs(gross_profit / gross_loss_abs) if gross_loss_abs != 0 else 999.0

    # 4. Average Holding Period (in bars/days)
    avg_holding_period = ta.len.average if 'len' in ta and 'average' in ta.len else 0

    return {
        '总交易次数': total_trades,
        '盈利交易次数': won,
        '亏损交易次数': lost,
        '胜率': f"{winrate:.2f}%",
        '平均持有期(周期)': f"{avg_holding_period:.1f}",
        '盈亏比(Profit Factor)': f"{profit_factor:.2f}",
        '平均盈利': ta.won.pnl.average if 'won' in ta and 'pnl' in ta.won and 'average' in ta.won else 0,
        '平均亏损': ta.lost.pnl.average if 'lost' in ta and 'pnl' in ta.lost and 'average' in ta.lost else 0,
    }


# ==============================================================================
# Backtrader 策略定义 - 【核心改造区域 1：固定买入 500 股】
# ==============================================================================
class MultiFactorStrategy(bt.Strategy):
    """
    Backtrader 多因子回测策略：
    买入逻辑：MACD金叉 且 CCI超卖反弹。
    卖出逻辑：MACD死叉 (或收盘平仓)。

    【改造点】：固定买入 500 股，忽略传入的 size 参数。
    """
    params = (
        ('fast', 12),
        ('slow', 26),
        ('signal', 9),
        ('cci_period', 14)
        # 移除 'size' 参数，改用硬编码的 500
    )

    # 【新增固定数量参数，方便查阅】
    FIXED_BUY_SIZE = 500

    def __init__(self):
        self.order = None
        self.indicators = {}
        # 策略只对添加到 Cerebro 的第一个（也是唯一一个）数据源进行指标计算
        d = self.datas[0]
        self.indicators[d] = {
            'macd': bt.indicators.MACD(d, period_me1=self.p.fast, period_me2=self.p.slow, period_signal=self.p.signal),
            'cci': bt.indicators.CCI(d, period=self.p.cci_period),
        }

    def next(self):
        data = self.datas[0]
        if not data.ready: return

        macd = self.indicators[data]['macd']
        cci = self.indicators[data]['cci']

        # 确保指标有足够数据
        if len(data) < max(self.p.slow, self.p.cci_period, 30):
            return

        # 仅在收盘价有效时进行操作
        if np.isnan(data.close[0]):
            return

        if self.getposition(data).size == 0:  # 未持仓
            # 买入条件: MACD金叉 AND CCI超卖反弹
            macd_golden_cross = (macd.macd[-1] < macd.signal[-1]) and (macd.macd[0] > macd.signal[0])
            cci_oversold_rebound = (cci.cci[-1] < -100) and (cci.cci[0] > -100)

            if macd_golden_cross and cci_oversold_rebound:
                # 【硬编码买入数量：500 股】
                self.order = self.buy(data=data, size=self.FIXED_BUY_SIZE)

        else:  # 已持仓
            # 卖出条件: MACD死叉
            macd_death_cross = (macd.macd[-1] > macd.signal[-1]) and (macd.macd[0] < macd.signal[0])

            if macd_death_cross:
                self.order = self.close(data=data)


# 回测执行函数 - 【核心改造区域 2：提高启动资金】
def run_backtrader_backtests(recommended_codes: list, hist_data_df: pd.DataFrame, stock_name_map: Dict[str, str]) -> \
        List[Dict[str, Any]]:
    """
    对多只股票进行独立回测，并返回每只股票的总结字典列表。
    【改造点】：将初始资金提高到 10,000,000.0，以支持固定买入 500 股的操作。
    """
    all_results = []

    if hist_data_df.empty:
        return [{'股票代码': 'N/A', '总收益率': '历史数据为空，回测失败'}]

    # 预处理历史数据列名和索引
    hist_data_df['日期'] = pd.to_datetime(hist_data_df['日期'])

    print(
        f"\n>>> 正在对 {len(recommended_codes)} 只高置信度股票进行独立回测 (固定买入 {MultiFactorStrategy.FIXED_BUY_SIZE} 股)...")

    # 【关键调整：设置一个远超实际需求的启动资金】
    # 目的：忽略资产配置，确保固定买入 500 股的操作不会因资金不足而失败。
    starting_cash = 10000000.0

    # 循环对每一只股票进行独立回测
    for i, code in enumerate(recommended_codes):
        stock_name = stock_name_map.get(code, 'N/A')

        # 1. 准备单只股票的数据
        stock_data = hist_data_df[hist_data_df['股票代码'] == code].copy()
        if stock_data.empty or len(stock_data) < 50:
            print(f"  - [WARN] {code} ({stock_name}) 数据不足 ({len(stock_data)} 条)，跳过。")
            continue

        stock_data.set_index('日期', inplace=True)
        # 确保数据列名符合 PandasData 要求
        stock_data.rename(columns={
            '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close',
            '成交量': 'volume', '成交额': 'openinterest'  # openinterest 实际用成交额或-1填充
        }, inplace=True)

        # 2. 初始化 Cerebro
        # 注意：此处每次循环都会创建一个新的 Cerebro 实例，确保了回测的独立性
        cerebro = bt.Cerebro(stdstats=False)

        # 3. 添加数据 feed
        data_feed = bt.feeds.PandasData(
            dataname=stock_data,
            datetime='index',
            open='open',
            high='high',
            low='low',
            close='close',
            volume='volume',
            openinterest=-1,  # -1 表示忽略该字段
            timeframe=bt.TimeFrame.Days
        )
        cerebro.adddata(data_feed, name=code)

        # 4. 设置资金和佣金
        cerebro.broker.setcash(starting_cash)
        cerebro.broker.setcommission(commission=0.0003)  # 示例佣金 0.03%
        # 【注意：此处不需要给策略传递 size 参数了】
        cerebro.addstrategy(MultiFactorStrategy)

        # 5. 添加分析器 (SharpeRatio, DrawDown, Returns, TradeAnalyzer, SQN)
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Years, riskfreerate=0.03)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='ddown')
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
        cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

        # 6. 运行并提取结果
        try:
            results = cerebro.run()
            strat = results[0]

            # 提取核心指标
            final_value = cerebro.broker.getvalue()
            ret_analyzer = strat.analyzers.returns.get_analysis()
            ddown_analyzer = strat.analyzers.ddown.get_analysis()
            sharpe_analyzer = strat.analyzers.sharpe.get_analysis()
            sqn_analyzer = strat.analyzers.sqn.get_analysis()
            trade_metrics = get_trade_metrics(strat.analyzers.ta)

            result_dict = {
                '股票代码': code,
                '股票简称': stock_name,  # 新增股票简称
                # 虽然关注个股，但为了报告完整性保留资金信息
                '初始资金': f"{starting_cash:,.2f} 元",
                '最终价值': f"{final_value:,.2f} 元",
                '总收益率': f"{(final_value / starting_cash - 1) * 100:.2f}%",
                '年化收益率': f"{ret_analyzer['rnorm100']:.2f}%" if 'rnorm100' in ret_analyzer else '0.00%',
                '夏普比率': f"{sharpe_analyzer['sharperatio']:.2f}" if 'sharperatio' in sharpe_analyzer and
                                                                       sharpe_analyzer[
                                                                           'sharperatio'] is not None else 'N/A',
                '系统质量指数(SQN)': f"{sqn_analyzer['sqn']:.2f}" if 'sqn' in sqn_analyzer and sqn_analyzer[
                    'sqn'] is not None else 'N/A',
                '最大回撤(Max Drawdown)': f"{ddown_analyzer.max.drawdown:.2f}%" if 'max' in ddown_analyzer and 'drawdown' in ddown_analyzer.max else '0.00%',
                '回撤持续期(周期)': ddown_analyzer.max.len if 'max' in ddown_analyzer and 'len' in ddown_analyzer.max else 0,
                '总交易次数': trade_metrics['总交易次数'],
                '盈利交易次数': trade_metrics['盈利交易次数'],
                '胜率': trade_metrics['胜率'],
                '平均持有期(周期)': trade_metrics['平均持有期(周期)'],
                '盈亏比(Profit Factor)': trade_metrics['盈亏比(Profit Factor)'],
                '平均盈利': f"{trade_metrics['平均盈利']:,.2f} 元" if trade_metrics['平均盈利'] != 0 and isinstance(
                    trade_metrics['平均盈利'], (int, float)) else '0.00 元',
                '平均亏损': f"{trade_metrics['平均亏损']:,.2f} 元" if trade_metrics['平均亏损'] != 0 and isinstance(
                    trade_metrics['平均亏损'], (int, float)) else '0.00 元',
            }
            all_results.append(result_dict)

        except Exception as e:
            print(f"    [ERROR] 回测 {code} ({stock_name}) 失败: {e}")
            all_results.append(
                {'股票代码': code, '股票简称': stock_name, '总收益率': '回测失败', '最大回撤(Max Drawdown)': 'N/A',
                 '夏普比率': 'N/A', '总交易次数': 0})

    if not all_results:
        return [{'股票代码': 'N/A', '总收益率': '没有符合回测条件的股票'}]

    # 直接返回列表，不进行 DataFrame 转换和排序
    return all_results


# ==============================================================================
# 剩余代码（保持不变）
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
                # 确保 '成交量' 可以被正确读取为数字，如果需要
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

    # FIX 3: 增加缓存文件 '日期' 列的检查
    def fetch_hist_data_parallel(self, codes: list, days: int) -> pd.DataFrame:
        """并行获取指定股票代码的历史数据，并缓存到本地文件。"""
        print(f"\n正在为 {len(codes)} 只股票下载 {days} 天的历史数据，使用5个线程并行处理。")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # 检查缓存文件
        if os.path.exists(self.macd_cache_file):
            cached_df = self.load_data_from_txt(self.macd_cache_file)
            if not cached_df.empty:

                # --- 修复区域：新增 '日期' 列存在性检查 ---
                if '日期' not in cached_df.columns:
                    print(
                        f"[WARN] 缓存文件 {os.path.basename(self.macd_cache_file)} 缺少 '日期' 列，缓存数据无效，将重新下载。")
                    # 不返回 cached_df，流程将继续进行重新下载
                else:
                    # 简单检查数据日期，如果满足要求则直接返回
                    if pd.to_datetime(cached_df['日期']).max() > end_date - timedelta(days=5):
                        print(f"发现历史数据缓存文件，直接加载。")
                        return cached_df
                    else:
                        print(f"缓存数据日期不新鲜，将重新下载。")
                # --- 修复区域结束 ---

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
                        # 确保成交量是数字
                        if '成交量' in hist_df.columns:
                            hist_df['成交量'] = pd.to_numeric(hist_df['成交量'], errors='coerce')
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
        # 价格别名，统一标准化到 '最新价'
        self.price_aliases = {'最新价': '最新价', '现价': '最新价'}

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 的列名，确保'股票代码'和'股票简称'等列存在。"""
        if df.empty:
            return df

        # 股票代码
        found_code_col = False
        for old_name, new_name in self.code_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                found_code_col = True
                break
        if not found_code_col:
            # 如果没有找到股票代码列，直接返回空DataFrame
            return pd.DataFrame()

        # 股票简称
        for old_name, new_name in self.name_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                break

        # 价格列：统一标准化为 '最新价'
        for old_name, new_name in self.price_aliases.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
                break

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
        if profit_df.empty or spot_df.empty or '股票代码' not in profit_df.columns:
            print("[WARN] 研报数据或实时行情数据为空，无法生成主力研报筛选表。")
            return pd.DataFrame()

        # 确保 spot_df 有 '当前价格' 列 (由 process_spot_data 负责创建)
        price_col = '当前价格'
        if price_col not in spot_df.columns:
            # 兼容处理：如果 spot_df 缺乏 '当前价格'，但有 '最新价'，则使用 '最新价'
            if '最新价' in spot_df.columns:
                price_col = '最新价'
            else:
                print(f"[WARN] 实时行情数据中缺乏 {price_col} 列，无法合并价格到主力研报表。")
                spot_df[price_col] = 'N/A'  # 保证合并能进行

        final_df = pd.merge(profit_df, spot_df[['股票代码', price_col]], on='股票代码', how='left')

        # 重命名为目标列名，方便报告生成
        if price_col != '最新价':
            final_df.rename(columns={price_col: '最新价'}, inplace=True)

        final_df['最新价'] = final_df['最新价'].fillna('N/A')

        # 添加完整股票编码
        if '完整股票编码' not in final_df.columns:
            final_df['完整股票编码'] = final_df['股票代码'].apply(format_stock_code)

        final_df['股票链接'] = 'https://hybrid.gelonghui.com/stock-check/' + final_df['完整股票编码'].astype(
            str).str.lower()
        final_cols = ['股票代码', '完整股票编码', '股票链接', '最新价']
        other_cols = [col for col in final_df.columns if col not in final_cols]
        return final_df[final_cols + other_cols]

    def process_spot_data(self, spot_data_all: pd.DataFrame, filtered_codes_df: pd.DataFrame) -> pd.DataFrame:
        """处理实时行情数据，并确保价格列名为'当前价格'。"""
        if spot_data_all.empty or filtered_codes_df.empty:
            return pd.DataFrame()

        # 注意：此处 spot_data_all 已经被 clean_data 处理，价格列名为 '最新价'

        # 核心修复：确保价格列名为'当前价格'
        if '最新价' in spot_data_all.columns:
            # 重命名为 '当前价格'，确保下游 process_and_merge_xstp_data 可以正确使用
            spot_data_all.rename(columns={'最新价': '当前价格'}, inplace=True)
        elif '现价' in spot_data_all.columns:
            # 备用：如果 clean_data 没起作用，这里补救并重命名
            spot_data_all.rename(columns={'现价': '当前价格'}, inplace=True)

        # 确保合并后保留'股票代码'和'当前价格'
        if '当前价格' not in spot_data_all.columns:
            # 如果重命名失败，则无法进行价格过滤
            print("[WARN] 实时行情数据中缺乏价格列，无法生成过滤后的行情数据。")
            return pd.DataFrame()

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

    # FIX 2: 修正价格列合并时的 KeyError
    def process_and_merge_xstp_data(self, df20: pd.DataFrame, df60: pd.DataFrame, df90: pd.DataFrame,
                                    spot_data_all: pd.DataFrame) -> pd.DataFrame:
        """处理并合并20日、60日和90日均线数据，并添加实时价格过滤。"""
        print("正在处理并合并20日、60日和90日均线数据...")

        # 确保传入的均线数据进行清洗和重命名
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

        # --- 核心修复区域：确保实时价格列名为 '当前价格' 且存在 ---
        price_col_for_merge = None

        # 检查 spot_data_all 中是否存在 '当前价格'（由 process_spot_data 负责创建）
        # 或 '最新价'（由 clean_data 负责标准化，如果 process_spot_data 还没被调用）
        if '当前价格' in spot_data_all.columns:
            price_col_for_merge = '当前价格'
        elif '最新价' in spot_data_all.columns:
            price_col_for_merge = '最新价'

        if price_col_for_merge:
            # 创建一个用于合并的临时DataFrame，确保列名正确
            spot_data_temp = spot_data_all[['股票代码', price_col_for_merge]].copy()
            if price_col_for_merge != '当前价格':
                # 重命名为目标列名，防止 Key Error
                spot_data_temp.rename(columns={price_col_for_merge: '当前价格'}, inplace=True)

            # 2. 执行合并
            final_merged_df = pd.merge(final_merged_df, spot_data_temp, on='股票代码', how='left')
        else:
            # 价格数据缺失，无法合并，给一个空列
            final_merged_df['当前价格'] = np.nan
            print("[WARN] 实时行情数据中缺少价格列（最新价/现价/当前价格），无法进行价格合并。后续过滤将跳过。")

        # --- 核心修复区域结束 ---

        final_merged_df['20日均线最新价'] = pd.to_numeric(final_merged_df['20日均线最新价'], errors='coerce')
        final_merged_df['60日均线最新价'] = pd.to_numeric(final_merged_df['60日均线最新价'], errors='coerce')
        final_merged_df['90日均线最新价'] = pd.to_numeric(final_merged_df['90日均线最新价'], errors='coerce')
        # 确保 '当前价格' 存在才能转换类型
        if '当前价格' in final_merged_df.columns:
            final_merged_df['当前价格'] = pd.to_numeric(final_merged_df['当前价格'], errors='coerce')
        else:
            # 如果上面因缺失价格数据而跳过合并，这里补充一个空列防止报错
            final_merged_df['当前价格'] = np.nan

        # 集中执行所有过滤条件
        filtered_df = final_merged_df[
            (final_merged_df['股票简称'].notna()) &
            (final_merged_df['20日均线最新价'].notna()) &
            (final_merged_df['当前价格'].notna()) &
            (final_merged_df['当前价格'] > final_merged_df['20日均线最新价'])
            ].copy()
        # 添加多头排列条件
        filtered_df = filtered_df[
            (filtered_df['20日均线最新价'] > filtered_df['60日均线最新价']) &
            (filtered_df['60日均线最新价'] > filtered_df['90日均线最新价'])
            ].copy()
        filtered_df.fillna('N/A', inplace=True)
        print(f"均线多头排列过滤完成，剩余 {len(filtered_df)} 只股票。")
        return filtered_df

    def process_all_technical_indicators(self, all_ta_codes: list, hist_df_all: pd.DataFrame,
                                         source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """集中处理所有技术指标，避免重复计算。"""
        print(f"\n正在对 {len(all_ta_codes)} 只股票进行批量技术分析...")
        results = {'macd': [], 'cci': [], 'rsi': []}

        # 仅处理有历史数据的股票代码
        hist_df_all.dropna(subset=['股票代码'], inplace=True)
        grouped = hist_df_all.groupby('股票代码')

        # 定义需要重命名的列
        column_map = {'收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'}

        for code, group_df_raw in grouped:
            # 只对在 all_ta_codes 列表中的代码进行计算
            if code not in all_ta_codes:
                continue

            try:
                group_df = group_df_raw.copy()
                group_df.rename(columns=column_map, inplace=True)

                # 确保关键列为数字类型
                for col in ['close', 'high', 'low', 'volume']:
                    group_df[col] = pd.to_numeric(group_df[col], errors='coerce')

                group_df.dropna(subset=['close', 'high', 'low', 'volume'], inplace=True)

                if len(group_df) < 30:
                    continue

                # --- 1. MACD ---
                group_df.ta.macd(close=group_df['close'], append=True)
                if len(group_df) >= 2 and 'MACD_12_26_9' in group_df.columns:
                    last_day_macd = group_df.iloc[-1]
                    prev_day_macd = group_df.iloc[-2]
                    is_golden_cross = (prev_day_macd['MACD_12_26_9'] < prev_day_macd['MACDs_12_26_9']) and (
                            last_day_macd['MACD_12_26_9'] > last_day_macd['MACDs_12_26_9'])
                    if is_golden_cross:
                        # 从原始数据源获取股票简称
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0] if not source_df.empty and code in \
                                                                                         source_df[
                                                                                             '股票代码'].values else {
                            '股票简称': 'N/A'}
                        results['macd'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            'MACD (DIF)': f"{last_day_macd['MACD_12_26_9']:.2f}",
                            'MACD信号线 (DEA)': f"{last_day_macd['MACDs_12_26_9']:.2f}",
                            'MACD动能柱': f"{last_day_macd['MACDh_12_26_9']:.2f}",
                            'MACD买卖信号': '金叉 (买入信号)',
                        })
                # --- 2. CCI ---
                group_df.ta.cci(close=group_df['close'], high=group_df['high'], low=group_df['low'], append=True)
                if len(group_df) >= 2 and 'CCI_14_0.015' in group_df.columns:
                    last_day_cci = group_df.iloc[-1]
                    prev_day_cci = group_df.iloc[-2]
                    is_oversold_signal = prev_day_cci['CCI_14_0.015'] < -100 and last_day_cci['CCI_14_0.015'] > -100
                    if is_oversold_signal:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0] if not source_df.empty and code in \
                                                                                         source_df[
                                                                                             '股票代码'].values else {
                            '股票简称': 'N/A'}
                        results['cci'].append({
                            '股票代码': code,
                            '股票简称': stock_info.get('股票简称', 'N/A'),
                            '最新CCI值': f"{last_day_cci['CCI_14_0.015']:.2f}",
                            'CCI买卖信号': '超卖买入信号',
                        })
                # --- 3. RSI ---
                group_df.ta.rsi(length=6, append=True)
                group_df.ta.rsi(length=14, append=True)
                if len(group_df) >= 2 and 'RSI_6' in group_df.columns and 'RSI_14' in group_df.columns:
                    last_day_rsi = group_df.iloc[-1]
                    prev_day_rsi = group_df.iloc[-2]
                    is_golden_cross_rsi = (prev_day_rsi['RSI_6'] < prev_day_rsi['RSI_14']) and (
                            last_day_rsi['RSI_6'] > last_day_rsi['RSI_14'])
                    is_rsi14_in_range = 60 <= last_day_rsi['RSI_14'] <= 80
                    if is_golden_cross_rsi and is_rsi14_in_range:
                        stock_info = source_df[source_df['股票代码'] == code].iloc[0] if not source_df.empty and code in \
                                                                                         source_df[
                                                                                             '股票代码'].values else {
                            '股票简称': 'N/A'}
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
        return {'macd_df': macd_df, 'cci_df': cci_df, 'rsi_df': rsi_df}

    def find_recommended_stocks_with_score(self, macd_df: pd.DataFrame, cci_df: pd.DataFrame, xstp_df: pd.DataFrame,
                                           rsi_df: pd.DataFrame, strong_stocks_df: pd.DataFrame,
                                           filtered_spot: pd.DataFrame,
                                           consecutive_rise_df: pd.DataFrame) -> pd.DataFrame:
        """基于多因子评分筛选推荐股票。"""
        print("\n正在基于多因子评分筛选推荐股票...")

        # 确保所有用于合并的DF都有 '股票代码' 和 '股票简称' 且不为空
        def get_valid_df(df):
            if df is not None and not df.empty and '股票代码' in df.columns and '股票简称' in df.columns:
                return df[['股票代码', '股票简称']].copy()
            return pd.DataFrame(columns=['股票代码', '股票简称'])

        macd_temp = get_valid_df(macd_df)
        cci_temp = get_valid_df(cci_df)
        xstp_temp = get_valid_df(xstp_df)
        rsi_temp = get_valid_df(rsi_df)

        all_codes = pd.concat([macd_temp, cci_temp, xstp_temp, rsi_temp],
                              ignore_index=True).drop_duplicates()

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
            for _, row in source_df.iterrows():
                code = row['股票代码']
                # 使用 loc 进行赋值，避免 SettingWithCopyWarning
                if code in final_df['股票代码'].values:
                    idx = final_df.index[final_df['股票代码'] == code].tolist()
                    final_df.loc[idx, '符合条件数量'] += 1
                    if check_col:
                        final_df.loc[idx, column_name] = row[check_col]
                    else:
                        final_df.loc[idx, column_name] = '已满足'

        # 确保原始 DataFrame 中包含所需的列
        if 'MACD买卖信号' in macd_df.columns:
            update_df(macd_df, 'MACD买卖信号', 'MACD买卖信号')
        if 'CCI买卖信号' in cci_df.columns:
            update_df(cci_df, 'CCI买卖信号', 'CCI买卖信号')
        if 'RSI买卖信号' in rsi_df.columns:
            update_df(rsi_df, 'RSI买卖信号', 'RSI买卖信号')
        if '股票代码' in xstp_df.columns:  # xstp_df 只有 '股票代码' 列，不需 check_col
            update_df(xstp_df, '均线多头排列')

        strong_stocks_codes = set(strong_stocks_df['股票代码']) if '股票代码' in strong_stocks_df.columns else set()
        final_df['强势股池'] = final_df['股票代码'].apply(lambda x: 'YES' if x in strong_stocks_codes else 'NO')

        # 新增：合并连涨天数数据
        if not consecutive_rise_df.empty and '连涨天数' in consecutive_rise_df.columns:
            consecutive_rise_df['连涨天数'] = pd.to_numeric(consecutive_rise_df['连涨天数'], errors='coerce')
            final_df = pd.merge(final_df, consecutive_rise_df[['股票代码', '连涨天数']], on='股票代码', how='left')
            final_df['连涨天数'] = final_df['连涨天数'].fillna(0).astype(int)
        else:
            final_df['连涨天数'] = 0

        # 合并最新价，使用filtered_spot中的 '当前价格' 列
        if '当前价格' in filtered_spot.columns:
            final_df = pd.merge(final_df, filtered_spot[['股票代码', '当前价格']], on='股票代码', how='left')
        else:
            final_df['当前价格'] = 'N/A'

        final_df['当前价格'] = final_df['当前价格'].fillna('N/A')

        # 修复股票链接生成
        final_df['股票链接'] = final_df['股票代码'].apply(
            lambda x: f'https://hybrid.gelonghui.com/stock-check/{format_stock_code(x)}'.lower()
        )

        recommended_df = final_df[final_df['符合条件数量'] >= 1].sort_values(by='符合条件数量',
                                                                             ascending=False).reset_index(
            drop=True)
        recommended_df.fillna('N/A', inplace=True)
        print(f"成功筛选出 {len(recommended_df)} 只最终推荐股票，并按符合条件数量排序。")
        return recommended_df


# ==============================================================================
# Excel报告生成类 (用于主报告)
# ==============================================================================
class ExcelReporter:
    """
    负责将处理后的数据导出为结构化的Excel报告。
    """

    def __init__(self, config: Config):
        self.config = config
        self.file_path = os.path.join(self.config.SAVE_DIRECTORY,
                                      f"主力研报筛选_{datetime.now().strftime('%Y%m%d')}.xlsx")
        # Ensure the directory exists
        os.makedirs(self.config.SAVE_DIRECTORY, exist_ok=True)
        self.writer = None  # Will be initialized in generate_report

        # Create temporary formats
        self.temp_workbook = xlsxwriter.Workbook('temp.xlsx')
        self.header_format = self.temp_workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
        self.text_format = self.temp_workbook.add_format({'border': 1})
        self.link_format = self.temp_workbook.add_format({'border': 1, 'font_color': 'blue', 'underline': 1})
        self.red_format = self.temp_workbook.add_format({'border': 1, 'font_color': 'red'})
        self.green_format = self.temp_workbook.add_format({'border': 1, 'font_color': 'green'})
        self.temp_workbook.close()
        os.remove('temp.xlsx')  # Clean up

    def _write_dataframe(self, df: pd.DataFrame, sheet_name: str, writer, workbook, link_col: str = None,
                         conditional_format: Dict[str, Any] = None):
        """通用写入DataFrame到Excel的方法。"""
        if df.empty:
            print(f"[WARN] {sheet_name} 数据为空，跳过生成该工作表。")
            return
        try:
            worksheet = workbook.add_worksheet(sheet_name)
            df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

            # Recreate formats in the actual workbook
            header_format = workbook.add_format(
                {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
            text_format = workbook.add_format({'border': 1})
            red_format = workbook.add_format({'border': 1, 'font_color': 'red'})
            green_format = workbook.add_format({'border': 1, 'font_color': 'green'})

            for col_num, value in enumerate(df.columns):
                worksheet.write(0, col_num, value, header_format)
                col_width = 15
                if '链接' in value or '简称' in value or '名称' in value:
                    col_width = 25
                elif '最终价值' in value or '初始资金' in value:
                    col_width = 18
                # 针对链接列，给足够的宽度显示 URL
                if '链接' in value:
                    col_width = 40
                worksheet.set_column(col_num, col_num, col_width, text_format)

            # 直接写入 URL 字符串
            if link_col and link_col in df.columns:
                link_col_idx = df.columns.get_loc(link_col)
                for row_num, link in enumerate(df[link_col], 1):
                    if link and link != 'N/A':
                        worksheet.write(row_num, link_col_idx, link, text_format)
                    else:
                        worksheet.write(row_num, link_col_idx, 'N/A', text_format)

            if conditional_format:
                for condition in conditional_format:
                    col_name = condition['column']
                    col_format = condition['format']
                    if col_name in df.columns:
                        col_idx = df.columns.get_loc(col_name)
                        for row_num, value in enumerate(df[col_name], 1):
                            cell_value = df.iloc[row_num - 1][col_name]
                            if condition['check'](cell_value):
                                # 使用原始值写入，但应用条件格式
                                worksheet.write(row_num, col_idx, cell_value, col_format)
        except Exception as e:
            print(f"[ERROR] 写入工作表 {sheet_name} 时出错: {e}")

    def generate_report(self, sheets_data: Dict[str, pd.DataFrame]):
        """生成最终的Excel报告。"""
        print("\n>>> 正在生成主Excel报告...")

        sheet_specs = {
            # 保留标的回测的**摘要**，但使用 DataFrame 格式
            '标的回测': {'df': sheets_data.get('标的回测'), 'link_col': None, 'conditional_format': [
                {'column': '总收益率', 'check': lambda x: str(x).endswith('%') and float(
                    str(x).replace('%', '').replace(' ', '').replace(',', '')) > 0, 'format': self.red_format},
                {'column': '总收益率', 'check': lambda x: str(x).endswith('%') and float(
                    str(x).replace('%', '').replace(' ', '').replace(',', '')) < 0, 'format': self.green_format},
            ]},
            '指标汇总': {'df': sheets_data.get('指标汇总'), 'link_col': '股票链接', 'conditional_format': [
                {'column': 'MACD买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format},
                {'column': 'CCI买卖信号', 'check': lambda x: '超卖' in str(x), 'format': self.green_format},
                {'column': 'RSI买卖信号', 'check': lambda x: '金叉' in str(x), 'format': self.red_format}]},
            '主力研报筛选': {'df': sheets_data.get('主力研报筛选'), 'link_col': '股票链接', 'conditional_format': None},
            'MACD金叉': {'df': sheets_data.get('MACD金叉'), 'link_col': None, 'conditional_format': None},
            'CCI超卖': {'df': sheets_data.get('CCI超卖'), 'link_col': None, 'conditional_format': None},
            'RSI金叉': {'df': sheets_data.get('RSI金叉'), 'link_col': None, 'conditional_format': None},
            '均线多头排列': {'df': sheets_data.get('均线多头排列'), 'link_col': None, 'conditional_format': None},
            '强势股池': {'df': sheets_data.get('强势股池'), 'link_col': None, 'conditional_format': None},
            '连续上涨': {'df': sheets_data.get('连续上涨'), 'link_col': None, 'conditional_format': None},
            '财务摘要数据': {'df': sheets_data.get('财务摘要数据'), 'link_col': None, 'conditional_format': None},
            '实时行情': {'df': sheets_data.get('实时行情'), 'link_col': None, 'conditional_format': None},
            '市场资金流向': {'df': sheets_data.get('市场资金流向'), 'link_col': None, 'conditional_format': None},
            '前十板块成分股': {'df': sheets_data.get('前十板块成分股'), 'link_col': None, 'conditional_format': None},
        }

        try:
            with pd.ExcelWriter(self.file_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                # 写入所有 Sheets
                for sheet_name, spec in sheet_specs.items():
                    if spec['df'] is not None and not spec['df'].empty:
                        # 传递 writer 和 workbook
                        self._write_dataframe(spec['df'], sheet_name, writer, workbook, spec.get('link_col'),
                                              spec.get('conditional_format'))

            print(f"主报告已成功生成: {self.file_path}")
        except Exception as e:
            print(f"[ERROR] 生成主Excel报告时出错: {e}")
            raise


# ==============================================================================
# 独立回测报告生成类
# ==============================================================================
class BacktestReporter:
    """
    负责将每只股票的独立回测结果导出为单独的Excel报告，每股一个Sheet。
    """

    def __init__(self, config: Config):
        self.config = config
        self.file_path = os.path.join(self.config.SAVE_DIRECTORY,
                                      f"回测报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        os.makedirs(self.config.SAVE_DIRECTORY, exist_ok=True)

    def generate_backtest_report(self, backtest_results: List[Dict[str, Any]]):
        """
        生成独立回测报告，每只股票一个 Sheet。
        backtest_results 是包含每只股票总结字典的列表。
        """
        if not backtest_results or (
                len(backtest_results) == 1 and backtest_results[0].get('总收益率') == '未能筛选出符合回测条件的股票'):
            print("[WARN] 无符合回测条件的股票，跳过生成独立回测报告。")
            return

        print(f"\n>>> 正在生成独立回测报告: {self.file_path}...")
        try:
            with pd.ExcelWriter(self.file_path, engine='xlsxwriter') as writer:
                workbook = writer.book

                # 格式定义
                header_format = workbook.add_format(
                    {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9D9D9'})
                text_format = workbook.add_format({'border': 1})
                red_format = workbook.add_format({'border': 1, 'font_color': 'red'})
                green_format = workbook.add_format({'border': 1, 'font_color': 'green'})

                for result in backtest_results:
                    code = result['股票代码']
                    name = result.get('股票简称', code)

                    # 1. 创建单行 DataFrame
                    df = pd.DataFrame([result])

                    # 2. 创建 Sheet 名称 (限制长度并进行安全替换)
                    sheet_name = f"{code}_{name}"
                    sheet_name = sheet_name[:31]  # Excel sheet name max length is 31
                    # 替换 Excel 不允许的字符
                    sheet_name = sheet_name.replace('\\', '_').replace('/', '_').replace('?', '_').replace('*',
                                                                                                           '_').replace(
                        '[', '_').replace(']', '_').replace(':', '_')

                    worksheet = workbook.add_worksheet(sheet_name)
                    df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

                    # 3. 写入表头和设置列宽
                    for col_num, value in enumerate(df.columns):
                        worksheet.write(0, col_num, value, header_format)
                        col_width = 18
                        if '收益率' in value: col_width = 15
                        worksheet.set_column(col_num, col_num, col_width, text_format)

                    # 4. 应用条件格式 (总收益率)
                    col_name = '总收益率'
                    if col_name in df.columns:
                        col_idx = df.columns.get_loc(col_name)
                        value = df.iloc[0][col_name]
                        if str(value).endswith('%') and str(value) != '回测失败':
                            # 尝试转换为浮点数进行比较，移除千位分隔符和百分号
                            try:
                                float_value = float(str(value).replace('%', '').replace(' ', '').replace(',', ''))
                                if float_value > 0:
                                    worksheet.write(1, col_idx, value, red_format)
                                elif float_value < 0:
                                    worksheet.write(1, col_idx, value, green_format)
                                else:
                                    worksheet.write(1, col_idx, value, text_format)
                            except ValueError:
                                worksheet.write(1, col_idx, value, text_format)  # 非数字格式的收益率
                        else:
                            worksheet.write(1, col_idx, value, text_format)

                print(f"独立回测报告已成功生成: {self.file_path}")
        except Exception as e:
            print(f"[ERROR] 写入独立回测报告时出错: {e}")
            raise


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
        self.bt_reporter = BacktestReporter(self.config)

    def run(self):
        """执行整个分析流程。"""
        start_time = time.time()
        print(f">>> 股票数据分析流程启动... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        sheets_data = {}

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

            # processed_spot_data 在此后会作为输入传递给 process_spot_data，
            # 并被 process_spot_data 修改 in-place (列名变为 '当前价格')
            filtered_spot = self.processor.process_spot_data(processed_spot_data, processed_profit_data)

            # 注意：此处 processed_spot_data 的价格列名已变为 '当前价格'
            main_report_sheet = self.processor.process_main_report_sheet(processed_profit_data, processed_spot_data)

            processed_financial_abstract = self.processor.process_financial_abstract(financial_abstract_df)
            processed_market_fund_flow = self.processor.process_market_fund_flow(market_fund_flow_raw)
            processed_strong_stocks = self.processor.process_general_rank(strong_stocks_df_raw, '强势股池')
            processed_consecutive_rise = self.processor.process_general_rank(consecutive_rise_df_raw, '连续上涨')
            top_industry_cons_df = self.fetcher.get_top_industry_stocks()

            # 将具有 '当前价格' 列的 processed_spot_data 传入
            processed_xstp_df = self.processor.process_and_merge_xstp_data(df_ma20, df_ma60, df_ma90,
                                                                           processed_spot_data)

            # --- FIX 2: 修正因空 DataFrame 导致的 KeyError '股票代码' ---
            # 合并所有需要进行技术分析的股票代码，只获取一次历史数据
            main_report_codes = set(main_report_sheet[
                                        '股票代码'].tolist()) if '股票代码' in main_report_sheet.columns and not main_report_sheet.empty else set()
            df_ma20_codes = set(
                df_ma20['股票代码'].tolist()) if '股票代码' in df_ma20.columns and not df_ma20.empty else set()

            all_ta_codes = main_report_codes | df_ma20_codes
            all_ta_codes = [code for code in all_ta_codes if pd.notna(code)]
            # --- END FIX 2 ---

            # 获取近 3 年历史数据 (1000天)
            hist_df_all = self.fetcher.fetch_hist_data_parallel(codes=list(all_ta_codes), days=1000)

            # 确保 source_df 不为空，用于 process_all_technical_indicators
            combined_source_df = pd.concat([main_report_sheet, df_ma20]).drop_duplicates(
                subset=['股票代码']) if '股票代码' in main_report_sheet.columns else pd.DataFrame(columns=['股票代码'])

            technical_results = self.processor.process_all_technical_indicators(
                all_ta_codes, hist_df_all, combined_source_df)
            macd_df = technical_results['macd_df']
            cci_df = technical_results['cci_df']
            rsi_df = technical_results['rsi_df']

            recommended_stocks = self.processor.find_recommended_stocks_with_score(
                macd_df, cci_df, processed_xstp_df, rsi_df, processed_strong_stocks,
                filtered_spot, processed_consecutive_rise)

            # 执行独立回测，并生成独立报告
            if not recommended_stocks.empty:
                high_confidence_stocks = recommended_stocks[recommended_stocks['符合条件数量'] >= 2]
                recommended_codes_for_bt = high_confidence_stocks['股票代码'].tolist()
                code_to_name_map = high_confidence_stocks.set_index('股票代码')['股票简称'].to_dict()
            else:
                recommended_codes_for_bt = []
                code_to_name_map = {}

            if recommended_codes_for_bt and not hist_df_all.empty:
                # 1. 执行回测，返回结果列表
                backtest_results_list = run_backtrader_backtests(recommended_codes_for_bt, hist_df_all,
                                                                 code_to_name_map)

                # 2. 生成独立回测报告（每股一个 Sheet）
                self.bt_reporter.generate_backtest_report(backtest_results_list)

                # 3. 将结果列表转换为 DataFrame，用于主报告的摘要 Sheet
                backtest_results_df_summary = pd.DataFrame(backtest_results_list)
                # 对摘要进行排序，方便查看
                backtest_results_df_summary = backtest_results_df_summary.sort_values(by=['总交易次数', '总收益率'],
                                                                                      ascending=[False, False])
            else:
                backtest_results_df_summary = pd.DataFrame(
                    [{'股票代码': 'N/A', '总收益率': '未能筛选出符合回测条件的股票'}])

            sheets_data = {
                '主力研报筛选': main_report_sheet, '财务摘要数据': processed_financial_abstract,
                '实时行情': filtered_spot, '行业板块': industry_board_df,
                '市场资金流向': processed_market_fund_flow, '前十板块成分股': top_industry_cons_df,
                '均线多头排列': processed_xstp_df,
                '强势股池': processed_strong_stocks, '连续上涨': processed_consecutive_rise,
                'MACD金叉': macd_df, 'CCI超卖': cci_df, 'RSI金叉': rsi_df,
                '指标汇总': recommended_stocks,
                '标的回测': backtest_results_df_summary,
            }

            self.reporter.generate_report(sheets_data)

        except Exception as e:
            print(f"[FATAL] 致命错误：数据分析流程意外终止。原因: {e}")
            # 如果是致命错误，仍然需要 raise 才能看到完整的 traceback
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