import pymysql


def truncate_table(table_name):
    """
    清空指定表并重置主键自增计数器（从1开始）

    :param table_name: 要清空的表名（字符串）
    """
    try:
        # 数据库连接信息
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root',
            database='stock',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 执行清空表操作
        sql = f"TRUNCATE TABLE `{table_name}`"
        cursor.execute(sql)

        conn.commit()
        print(f"✅ 表 `{table_name}` 已成功清空，id 已重置。")

    except Exception as e:
        print(f"❌ 清空表 `{table_name}` 时出错：{e}")

    finally:
        cursor.close()
        conn.close()


truncate_table('stock_info')
