# -*- coding: utf-8 -*-
"""
数据库连接与英雄胜率统计脚本
功能：计算英雄胜率统计，支持定时任务执行
需要安装：pymysql, pandas, sqlalchemy, openpyxl, apscheduler
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import sys

# 导入配置文件
try:
    from config import (
        DB_CONFIG, MIN_MATCHES, OUTPUT_FILE, LOG_FILE, 
        SCHEDULE_INTERVAL, ANALYST_NAME, LOG_LEVEL
    )
except ImportError:
    print("错误：找不到config.py文件，请确保config.py在同一目录下")
    sys.exit(1)

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志系统：同时输出到屏幕和文件"""
    # 创建logger
    logger = logging.getLogger('hero_winrate')
    
    # 设置日志级别
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # 清除已有的handler，避免重复
    if logger.handlers:
        logger.handlers.clear()
    
    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 文件处理器
    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"创建日志文件失败: {e}")
    
    # 屏幕处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# ==================== 数据库连接函数 ====================
def get_db_engine(config):
    """
    创建数据库引擎
    :param config: 数据库配置字典
    :return: SQLAlchemy 引擎对象
    """
    conn_str = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
        f"?charset={config['charset']}"
    )
    engine = create_engine(conn_str)
    return engine

def test_db_connection(engine, logger):
    """
    测试数据库连接
    :param engine: 数据库引擎
    :param logger: 日志对象
    :return: 连接成功返回True，否则返回False
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("数据库连接成功")
        return True
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return False

# ==================== 数据处理函数 ====================
def calculate_hero_winrate(engine, logger):
    """
    计算英雄胜率统计
    表结构：
    - hero: hero_id, hero_name, role, attack_type
    - battle_record: record_id, hero_id, is_win, battle_date
    :param engine: 数据库引擎
    :param logger: 日志对象
    :return: DataFrame 包含英雄胜率统计结果
    """
    query = f"""
        SELECT 
            h.hero_id,
            h.hero_name,
            COUNT(b.record_id) AS total_matches,
            SUM(b.is_win) AS wins,
            ROUND(SUM(b.is_win) * 100.0 / COUNT(b.record_id), 1) AS win_rate
        FROM hero h
        INNER JOIN battle_record b ON h.hero_id = b.hero_id
        GROUP BY h.hero_id, h.hero_name
        HAVING total_matches >= {MIN_MATCHES}
        ORDER BY win_rate DESC
    """
    
    try:
        df = pd.read_sql(query, con=engine)
        logger.info(f"成功获取英雄胜率数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"查询数据失败: {e}")
        raise

def add_analysis_info(df):
    """
    添加分析师信息和运行时间列
    :param df: 原始DataFrame
    :return: 添加了分析师信息的DataFrame
    """
    df = df.copy()
    df['analyst'] = ANALYST_NAME
    df['run_time'] = datetime.now()
    return df

def save_to_analysis_log(engine, df, logger):
    """
    将分析结果写入数据库的 analysis_log 表
    :param engine: 数据库引擎
    :param df: 要写入的DataFrame
    :param logger: 日志对象
    """
    try:
        # 调整字段名以匹配数据库表
        df_to_save = pd.DataFrame({
            'hero_id': df['hero_id'],
            'hero_name': df['hero_name'],
            'total_games': df['total_matches'],
            'win_games': df['wins'],
            'win_rate': df['win_rate'] / 100.0,  # 转换为小数
            'analyst': ANALYST_NAME,
            'run_time': datetime.now()
        })
        
        df_to_save.to_sql('analysis_log', con=engine, if_exists='append', index=False)
        logger.info(f"成功写入 {len(df_to_save)} 条记录到 analysis_log 表")
        return True
    except Exception as e:
        logger.error(f"写入数据库时出错: {e}")
        return False

def export_to_excel(df, file_path, logger):
    """
    导出DataFrame到Excel文件
    :param df: 要导出的DataFrame
    :param file_path: 导出文件路径
    :param logger: 日志对象
    :return: 导出成功返回True，否则返回False
    """
    try:
        df.to_excel(file_path, index=False, sheet_name="英雄胜率")
        logger.info(f"结果已导出至: {file_path}")
        return True
    except Exception as e:
        logger.error(f"导出Excel失败: {e}")
        return False

def print_summary(df, logger):
    """
    打印统计摘要
    :param df: 英雄胜率DataFrame
    :param logger: 日志对象
    """
    if df.empty:
        logger.warning(f"没有符合条件（总场次>={MIN_MATCHES}）的英雄数据")
        return
    
    total_heroes = len(df)
    avg_win_rate = df['win_rate'].mean()
    top_hero = df.iloc[0]['hero_name']
    top_win_rate = df.iloc[0]['win_rate']
    
    logger.info(f"统计摘要 - 符合条件的英雄总数: {total_heroes}")
    logger.info(f"统计摘要 - 平均胜率: {avg_win_rate:.1f}%")
    logger.info(f"统计摘要 - 胜率最高的英雄: {top_hero} ({top_win_rate:.1f}%)")
    
    # 打印数据预览
    logger.info("数据预览（前5行）：")
    for idx, row in df.head(5).iterrows():
        logger.info(f"  {row['hero_id']}. {row['hero_name']} - "
                   f"场次:{row['total_matches']}, 胜场:{row['wins']}, 胜率:{row['win_rate']}%")

def query_my_latest_result(engine, logger):
    """
    查询指定分析师的最新一次分析结果
    """
    query = f"""
        SELECT * FROM analysis_log 
        WHERE analyst = '{ANALYST_NAME}'
          AND run_time = (
              SELECT MAX(run_time) 
              FROM analysis_log 
              WHERE analyst = '{ANALYST_NAME}'
          )
        ORDER BY win_rate DESC
    """
    
    try:
        df = pd.read_sql(query, con=engine)
        
        if not df.empty:
            logger.info(f"查询到最新分析结果，共 {len(df)} 条记录，分析时间: {df['run_time'].iloc[0]}")
            top5 = df.head(5)[['hero_name', 'total_games', 'win_games', 'win_rate']].copy()
            top5['win_rate'] = top5['win_rate'].apply(lambda x: f"{x*100:.1f}%")
            logger.info("胜率排名前5的英雄：")
            for idx, row in top5.iterrows():
                logger.info(f"  {row['hero_name']} - 场次:{row['total_games']}, "
                           f"胜场:{row['win_games']}, 胜率:{row['win_rate']}")
        else:
            logger.info("未找到您的分析记录")
        return df
    except Exception as e:
        logger.error(f"查询分析记录失败: {e}")
        return pd.DataFrame()

# ==================== 主任务函数 ====================
def execute_analysis(logger):
    """
    执行完整的数据分析任务
    :param logger: 日志对象
    """
    engine = None
    try:
        logger.info("=" * 50)
        logger.info("开始执行英雄胜率统计分析任务")
        
        # 创建数据库引擎
        engine = get_db_engine(DB_CONFIG)
        
        # 测试数据库连接
        if not test_db_connection(engine, logger):
            logger.error("数据库连接失败，终止任务")
            return
        
        # 获取英雄胜率数据
        df_result = calculate_hero_winrate(engine, logger)
        
        if df_result.empty:
            logger.warning("没有获取到有效数据")
            return
        
        # 导出Excel
        if export_to_excel(df_result, OUTPUT_FILE, logger):
            logger.info(f"共导出 {len(df_result)} 条英雄数据")
        
        # 打印统计摘要
        print_summary(df_result, logger)
        
        # 写入分析日志表
        save_to_analysis_log(engine, df_result, logger)
        
        # 查询自己最新一次的结果
        query_my_latest_result(engine, logger)
        
        logger.info("英雄胜率统计分析任务执行完成")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"任务执行过程中出现错误: {e}")
    finally:
        if engine:
            engine.dispose()
            logger.debug("数据库连接已关闭")

# ==================== 主函数 ====================
def main():
    """
    主函数：配置日志并执行定时任务
    """
    # 配置日志
    logger = setup_logging()
    logger.info("程序启动")
    
    # 显示配置信息
    logger.info(f"配置信息 - 数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"配置信息 - 用户名: {DB_CONFIG['user']}")
    logger.info(f"配置信息 - 分析师: {ANALYST_NAME}")
    logger.info(f"配置信息 - 最小场次筛选: {MIN_MATCHES}")
    logger.info(f"配置信息 - 输出文件: {OUTPUT_FILE}")
    logger.info(f"配置信息 - 日志文件: {LOG_FILE}")
    logger.info(f"配置信息 - 定时任务间隔: {SCHEDULE_INTERVAL}秒（{SCHEDULE_INTERVAL/60:.0f}分钟）")
    
    # 创建定时任务调度器
    scheduler = BlockingScheduler()
    
    # 添加定时任务：每 SCHEDULE_INTERVAL 秒执行一次
    scheduler.add_job(
        execute_analysis, 
        'interval', 
        seconds=SCHEDULE_INTERVAL,
        args=[logger],
        id='hero_winrate_job',
        replace_existing=True
    )
    
    # 立即执行一次，验证功能
    logger.info("立即执行一次任务进行验证...")
    execute_analysis(logger)
    
    # 启动定时任务
    logger.info(f"启动定时任务，每 {SCHEDULE_INTERVAL} 秒执行一次（按 Ctrl+C 停止）...")
    logger.info("=" * 60)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("用户中断，程序退出")
    except Exception as e:
        logger.error(f"调度器运行出错: {e}")
    finally:
        logger.info("程序结束")

# ==================== 程序入口 ====================
if __name__ == "__main__":
    main()