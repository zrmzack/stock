import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text


def save_one_stock_info_to_db(stock_code, start_date, end_date):
    try:
        # 获取所有A股股票代码和名称
        stock_list_df = ak.stock_info_a_code_name()

        # 查找股票名称
        stock_name_result = stock_list_df.loc[stock_list_df["code"] == stock_code, "name"]
        if stock_name_result.empty:
            print(f"❌ 未找到股票代码 {stock_code} 的名称，跳过。")
            return
        stock_name = stock_name_result.values[0]

        # 获取行情数据
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

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

        # 添加代码和名称列
        df["stock_id"] = stock_code
        df["stock_name"] = stock_name

        # 转换时间格式
        df["stock_time"] = pd.to_datetime(df["stock_time"]).dt.date

        # 按列顺序重排
        df = df[[
            "stock_time", "stock_id", "stock_name", "stock_kp", "stock_sp", "stock_zg",
            "stock_zd", "stock_cje", "stock_zf", "stock_zdf", "stock_zde", "stock_hsl"
        ]]

        # 数据库连接信息
        db_config = {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "root",
            "database": "stock",
            "charset": "utf8mb4"
        }

        # 创建 SQLAlchemy 引擎
        engine_str = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
        engine = create_engine(engine_str)

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

        # 过滤重复
        df = df[~df.set_index(["stock_time", "stock_id"]).index.isin(existing)]

        # 插入新数据
        if not df.empty:
            df.to_sql(name='stock_info', con=engine, if_exists='append', index=False)
            print(f"✅ 股票 {stock_code}（{stock_name}）写入成功，新插入 {len(df)} 条。")
        else:
            print(f"⚠️ 股票 {stock_code}（{stock_name}）无新增数据，全部已存在。")

    except Exception as e:
        print(f"❌ 股票 {stock_code} 处理失败：{e}")


# 示例调用
save_one_stock_info_to_db("000001", "20250514", "20250515")
