import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
import time


def save_stock_info_to_db(stock_code, stock_name, start_date, end_date, engine):
    try:
        # 获取行情数据
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

        if df.empty:
            print(f"⚠️ 股票 {stock_code}（{stock_name}）无数据，跳过。")
            return

        # 字段映射
        df.rename(columns={
            "日期": "stock_time",
            "开盘": "stock_kp",
            "收盘": "stock_sp",
            "最高": "stock_zg",
            "最低": "stock_zd",
            "成交额": "stock_cje",
            "振幅": "stock_zf",
            "涨跌幅": "stock_zdf",
            "涨跌额": "stock_zde",
            "换手率": "stock_hsl"
        }, inplace=True)

        df["stock_id"] = stock_code
        df["stock_name"] = stock_name
        df["stock_time"] = pd.to_datetime(df["stock_time"]).dt.date

        df = df[[
            "stock_time", "stock_id", "stock_name", "stock_kp", "stock_sp", "stock_zg",
            "stock_zd", "stock_cje", "stock_zf", "stock_zdf", "stock_zde", "stock_hsl"
        ]]

        # 查询已有数据
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT stock_time, stock_id FROM stock_info
                WHERE stock_id = :stock_id AND stock_time IN :stock_time_list
            """), {
                "stock_id": stock_code,
                "stock_time_list": tuple(df["stock_time"].tolist())
            })
            existing = set(result.fetchall())

        # 过滤已有记录
        df = df[~df.set_index(["stock_time", "stock_id"]).index.isin(existing)]

        if not df.empty:
            df.to_sql(name='stock_info', con=engine, if_exists='append', index=False)
            print(f"✅ 股票 {stock_code}（{stock_name}）写入成功，共 {len(df)} 条。")
        else:
            print(f"⚠️ 股票 {stock_code}（{stock_name}）无新增数据。")

    except Exception as e:
        print(f"❌ 股票 {stock_code}（{stock_name}）处理失败：{e}")


def save_all_a_stock_info(start_date, end_date, delay=3):
    # 获取所有A股股票列表
    stock_list_df = ak.stock_info_a_code_name()

    # 数据库连接信息
    db_config = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "stock",
        "charset": "utf8mb4"
    }

    engine_str = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                 f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
    engine = create_engine(engine_str)

    for index, row in stock_list_df.iloc[3822:].iterrows():
        stock_code = row["code"]
        stock_name = row["name"]

        save_stock_info_to_db(stock_code, stock_name, start_date, end_date, engine)
        time.sleep(delay)  # 延迟避免被封IP


# 示例：导入所有股票 2024-01-01 到 2024-05-14 的数据
save_all_a_stock_info("20230601", "20250514", delay=1)
