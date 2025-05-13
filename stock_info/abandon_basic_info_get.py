import requests
import pandas as pd


def get_eastmoney_stock_data(page=1, per_page=20):
    """
    :param page: 页码（默认第1页）
    :param per_page: 每页数量（默认20条，最大100）
    :return: 包含股票数据的DataFrame
    """
    # API基础地址
    url = "http://88.push2.eastmoney.com/api/qt/clist/get"

    # 请求头（模拟浏览器访问）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }

    params = {
        "pn": page,  # 页码
        "pz": per_page,  # 每页数量
        "po": "1",  # 固定参数
        "np": "1",  # 固定参数
        "fltt": "2",  # 价格精度控制
        "invt": "2",  # 投资类型
        "fid": "f3",  # 排序字段（f3-成交量排序）
        "fs": "m:0+t:6,m:0+t:80",  # 市场筛选（A股+北交所）
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        # 数据字段
        "_": "1627375556883"  # 时间戳防缓存
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # 检查HTTP错误

        data = response.json()
        df = pd.DataFrame(data["data"]["diff"])

        # 字段映射（部分核心字段）
        column_mapping = {
            "f12": "股票代码",
            "f14": "股票名称",
            "f2": "最新价",
            "f3": "涨跌幅",
            "f4": "涨跌额",
            "f5": "成交量(手)",
            "f6": "成交额(元)",
            "f7": "振幅",
            "f8": "换手率",
            "f15": "最高价",
            "f16": "最低价",
            "f17": "今开",
            "f18": "昨收",
            "f23": "市净率",
            "f24": "市盈率(动态)",
        }

        # 数据清洗
        return (
            df.rename(columns=column_mapping)
            .replace("-", pd.NA)
            .assign(更新时间=pd.to_datetime("now"))
        )

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {str(e)}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"数据解析错误，可能接口已更新: {str(e)}")
        return pd.DataFrame()


# 使用示例
if __name__ == "__main__":
    df = get_eastmoney_stock_data(page=1, per_page=10)

    if not df.empty:
        print("最新股票行情数据：")
        print(df[["股票代码", "股票名称", "最新价", "涨跌幅", "成交量(手)"]].head())
    else:
        print("数据获取失败")