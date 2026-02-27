#!/usr/bin/env python
# 初始化生产效率分析系统数据

import os
import shutil
from models import init_db

def main():
    """清理旧数据并初始化系统"""
    print("=== 生产效率分析系统初始化 ===")
    
    # 清理数据目录
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    if os.path.exists(data_dir):
        print(f"发现现有数据目录: {data_dir}")
        try:
            # 备份旧数据
            backup_dir = os.path.join(os.path.dirname(__file__), 'data_backup')
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
            
            # 创建备份目录
            os.makedirs(backup_dir, exist_ok=True)
            
            # 复制旧数据文件
            for file in os.listdir(data_dir):
                old_path = os.path.join(data_dir, file)
                if os.path.isfile(old_path):
                    new_path = os.path.join(backup_dir, file)
                    shutil.copy2(old_path, new_path)
                    print(f"备份文件: {file}")
            
            # 清理旧数据目录
            shutil.rmtree(data_dir)
            print("已清理旧数据目录")
        except Exception as e:
            print(f"备份/清理数据时出错: {e}")
    
    # 初始化数据库
    print("正在初始化数据...")
    init_db()
    
    print("\n=== 初始化完成 ===")
    print("现在你可以运行 python app.py 启动系统")

if __name__ == "__main__":
    main() 