# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pymysql
from datetime import datetime

# ========== 数据库连接 ==========
def connect_db():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='stock',
        charset='utf8mb4'
    )

# ========== 获取股票数据 ==========
def get_stock_data_by_code_or_name(stock_input, conn, table_name='stock_info'):
    if stock_input.isdigit():
        condition = f"stock_id = '{stock_input}'"
    else:
        condition = f"stock_name = '{stock_input}'"

    sql = f"""
    SELECT stock_time, stock_id, stock_name, stock_sp, stock_cje, stock_hsl
    FROM {table_name}
    WHERE {condition}
    ORDER BY stock_time ASC
    """
    df = pd.read_sql(sql, conn)
    df['stock_time'] = pd.to_datetime(df['stock_time'])
    return df

# ========== 指标计算 ==============
def compute_indicators(df):
    df['ma'] = df['stock_sp'].rolling(10).mean()
    df['std'] = df['stock_sp'].rolling(20).std()
    df['upper'] = df['ma'] + 2 * df['std']
    df['lower'] = df['ma'] - 2 * df['std']

    # MACD
    ema12 = df['stock_sp'].ewm(span=12, adjust=False).mean()
    ema26 = df['stock_sp'].ewm(span=26, adjust=False).mean()
    df['macd_diff'] = ema12 - ema26
    df['macd_dea'] = df['macd_diff'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd_diff'] - df['macd_dea']

    # RSI
    delta = df['stock_sp'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(14).mean()
    avg_loss = pd.Series(loss).rolling(14).mean()
    rs = avg_gain / (avg_loss + 1e-6)
    df['rsi'] = 100 - (100 / (1 + rs))

    df['avg_volume'] = df['stock_cje'].rolling(5).mean()
    return df

# ========== 策略核心函数 ==============
def smart_buy_signal_strategy(df):
    df = compute_indicators(df)
    df['ma_slope'] = df['ma'].diff()
    df['signal'] = 0

    # 条件优化：寻找底部特征
    cond_price_below_ma = df['stock_sp'] < df['ma']
    cond_boll_lower = df['stock_sp'] < df['lower']
    cond_boll_flat = df['upper'].rolling(5).std() < 0.3  # 缩口判断

    cond_macd_hist_increasing = df['macd_hist'] > df['macd_hist'].shift(1)
    cond_macd_cross = (df['macd_diff'] > df['macd_dea']) & (df['macd_diff'].shift(1) <= df['macd_dea'].shift(1))

    cond_rsi_rebound = (df['rsi'].shift(1) < 30) & (df['rsi'] > df['rsi'].shift(1))
    cond_turnover_ok = (df['stock_hsl'] > 0.5) & (df['stock_hsl'] < 5.0)

    # 总共满足 >= 4 个即买入
    condition = (
        cond_price_below_ma.astype(int) +
        cond_boll_lower.astype(int) +
        cond_boll_flat.astype(int) +
        cond_macd_hist_increasing.astype(int) +
        cond_macd_cross.astype(int) +
        cond_rsi_rebound.astype(int) +
        cond_turnover_ok.astype(int)
    ) >= 4

    df.loc[condition, 'signal'] = 1
    df.loc[~condition, 'signal'] = -1
    df['position'] = df['signal'].shift(1)
    return df

# ========== 判断函数 + 图形化 ==============
def check_single_stock_buy_signal(stock_input):
    conn = connect_db()
    try:
        df = get_stock_data_by_code_or_name(stock_input, conn)
        if len(df) < 30:
            print(f"❌ 股票 {stock_input} 数据不足，无法判断")
            return

        df = smart_buy_signal_strategy(df)
        today = datetime.today().date()
        today_signal = df[df['stock_time'].dt.date == today]

        if not today_signal.empty and today_signal.iloc[-1]['signal'] == 1:
            print(f"✅ 股票 {df.iloc[-1]['stock_name']}（{df.iloc[-1]['stock_id']}）今天出现买入信号！")
        else:
            print(f"📉 股票 {df.iloc[-1]['stock_name']}（{df.iloc[-1]['stock_id']}）今天没有买入信号。")

        print("\n📊 最近5日走势：")
        print(df[['stock_time', 'stock_sp', 'ma', 'rsi', 'signal']].tail(5))

        # 绘图展示
        plt.figure(figsize=(14, 6))
        plt.plot(df['stock_time'], df['stock_sp'], label='收盘价', color='black')
        plt.plot(df['stock_time'], df['ma'], label='均线', color='orange', linestyle='--')
        plt.plot(df['stock_time'], df['upper'], label='布林上轨', color='gray', linestyle=':')
        plt.plot(df['stock_time'], df['lower'], label='布林下轨', color='gray', linestyle=':')

        buy_signals = df[df['signal'] == 1]
        plt.scatter(buy_signals['stock_time'], buy_signals['stock_sp'], marker='^', color='green', label='买入信号', zorder=5)

        plt.title(f"股票{df.iloc[-1]['stock_id']}趋势图与买点")
        plt.xlabel("日期")
        plt.ylabel("股价")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"处理出错：{e}")
    finally:
        conn.close()

# 示例调用
check_single_stock_buy_signal("688646")
