**2025年10月13日更新**

1 当天如果已经完整的收集到临时文件MACD_hist_data_cache.txt的话，当天再去跑程序时允许直接加载数据。

2 通过自然时间校验发现数据非最新数据时，重新从东财获取最新数据并覆盖写入MACD_hist_data_cache.txt。

**2025年10月11日更新**

**1 分析出股票基础技术面基本面信息，并列举出报告**

主力研报筛选: main_report_sheet

财务摘要数据: processed_financial_abstract

实时行情: filtered_spot

行业板块: industry_board_df

市场资金流向: processed_market_fund_flow

前十板块成分股: top_industry_cons_df

均线多头排列: processed_xstp_df

向上突破: processed_xstp_df

强势股池: processed_strong_stocks

连续上涨: processed_consecutive_rise

MACD金叉: macd_df

CCI超卖: cci_df

RSI金叉: rsi_df

指标汇总: recommended_stocks
