import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random
import sys

# 导入数据处理器和混乱数据生成函数
from data_processor import DataProcessor
from generate_messy_test_data import (
    generate_messy_equipment_data,
    generate_messy_material_data,
    generate_messy_operation_data,
    generate_messy_environment_data
)

def test_messy_data_filtering():
    """测试系统对混乱数据的筛选能力"""
    print("\n========= 开始测试混乱数据筛选功能 =========")
    
    # 确保测试数据目录存在
    os.makedirs('test_data', exist_ok=True)
    
    # 创建测试结果记录
    results = {}
    
    # 测试设备数据筛选
    print("\n1. 测试设备数据筛选:")
    results['equipment'] = test_data_type('equipment')
    
    # 测试物料数据筛选
    print("\n2. 测试物料数据筛选:")
    results['material'] = test_data_type('material')
    
    # 测试人员操作数据筛选
    print("\n3. 测试人员操作数据筛选:")
    results['operation'] = test_data_type('operation')
    
    # 测试环境数据筛选
    print("\n4. 测试环境数据筛选:")
    results['environment'] = test_data_type('environment')
    
    # 显示总体测试结果
    print("\n========= 测试结果汇总 =========")
    success_count = sum(1 for r in results.values() if r['success'])
    print(f"总共测试 {len(results)} 种数据类型，成功 {success_count} 个")
    
    for data_type, result in results.items():
        icon = "✓" if result['success'] else "✗"
        print(f"{icon} {data_type}: 筛选前 {result['before']} 条，筛选后 {result['after']} 条，筛选率 {result['rate']}%")
    
    print("========= 测试完成 =========\n")

def test_data_type(data_type):
    """测试特定类型的数据筛选"""
    # 生成混乱数据
    df = generate_messy_data(data_type)
    
    if df is None or len(df) == 0:
        print(f"无法生成 {data_type} 类型的测试数据")
        return {'success': False, 'before': 0, 'after': 0, 'rate': 0}
    
    original_count = len(df)
    print(f"生成了 {original_count} 条混乱的 {data_type} 数据")
    
    # 保存到临时文件
    temp_file = f"test_data/messy_{data_type}_test.xlsx"
    df.to_excel(temp_file, index=False)
    print(f"已保存到临时文件: {temp_file}")
    
    # 使用数据处理器处理数据
    processor = DataProcessor()
    result = processor.process_file(temp_file, data_type)
    
    if not result['success']:
        print(f"处理 {data_type} 数据失败: {result.get('error', '未知错误')}")
        return {'success': False, 'before': original_count, 'after': 0, 'rate': 0}
    
    # 获取处理后的数据
    filtered_df = processor.processed_data[data_type]
    filtered_count = len(filtered_df)
    removed_count = original_count - filtered_count
    filter_rate = round((removed_count / original_count * 100), 2) if original_count > 0 else 0
    
    print(f"数据筛选结果: 筛选前 {original_count} 条，筛选后 {filtered_count} 条")
    print(f"被筛选掉 {removed_count} 条，筛选率 {filter_rate}%")
    
    # 验证筛选后的数据是否符合格式要求
    validation_success = validate_filtered_data(filtered_df, data_type)
    
    # 保存筛选后的数据到文件
    filtered_file = f"test_data/filtered_{data_type}_test.xlsx"
    filtered_df.to_excel(filtered_file, index=False)
    print(f"已保存筛选后的数据到: {filtered_file}")
    
    return {
        'success': validation_success,
        'before': original_count,
        'after': filtered_count,
        'rate': filter_rate
    }

def generate_messy_data(data_type, rows=100):
    """生成指定类型的混乱数据"""
    if data_type == 'equipment':
        return generate_messy_equipment_data(rows)
    elif data_type == 'material':
        return generate_messy_material_data(rows)
    elif data_type == 'operation':
        return generate_messy_operation_data(rows)
    elif data_type == 'environment':
        return generate_messy_environment_data(rows)
    return None

def validate_filtered_data(df, data_type):
    """验证筛选后的数据是否符合格式要求"""
    if len(df) == 0:
        print("警告: 筛选后数据为空")
        return False
    
    # 通用验证 - 检查是否有NaN值
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        print(f"警告: 筛选后数据仍包含 {null_counts.sum()} 个缺失值")
        print(null_counts[null_counts > 0])
    
    # 特定数据类型的验证
    try:
        if data_type == 'equipment':
            # 验证设备ID格式
            valid_device_ids = df['设备ID'].str.match(r'^CNC\d{3}$').all()
            # 验证设备状态值
            valid_statuses = df['设备状态'].isin(['运行中', '停机', '维护', '故障', '待机']).all()
            # 验证数值字段范围
            valid_runtime = ((df['总运行时间'] >= 0) & (df['总运行时间'] <= 1000)).all()
            valid_failures = ((df['故障次数'] >= 0) & (df['故障次数'] <= 100)).all()
            
            print(f"设备ID格式正确: {'是' if valid_device_ids else '否'}")
            print(f"设备状态值有效: {'是' if valid_statuses else '否'}")
            print(f"运行时间在有效范围: {'是' if valid_runtime else '否'}")
            print(f"故障次数在有效范围: {'是' if valid_failures else '否'}")
            
            return valid_device_ids and valid_statuses and valid_runtime and valid_failures
            
        elif data_type == 'material':
            # 验证物料编号格式
            valid_material_ids = df['物料编号'].str.match(r'^CNC\d{3}$').all()
            # 验证产品数量范围
            valid_quantities = ((df['产品数量'] >= 0) & (df['产品数量'] <= 10000)).all()
            # 验证合格产品数量不超过总数
            valid_qualified = (df['合格产品数量'] <= df['产品数量']).all()
            
            print(f"物料编号格式正确: {'是' if valid_material_ids else '否'}")
            print(f"产品数量在有效范围: {'是' if valid_quantities else '否'}")
            print(f"合格产品数量合理: {'是' if valid_qualified else '否'}")
            
            return valid_material_ids and valid_quantities and valid_qualified
            
        elif data_type == 'operation':
            # 验证工号格式
            valid_staff_ids = df['工号'].str.match(r'^W\d{3}$').all()
            # 验证操作类型
            valid_operations = df['操作类型'].isin(['上料', '下料', '维护', '质检', '调试', '设备清洁', '生产计划']).all()
            # 验证操作结果
            valid_results = df['操作结果'].isin(['正常', '异常']).all()
            # 验证熟练度范围
            valid_skills = ((df['熟练度'] >= 0) & (df['熟练度'] <= 1)).all()
            
            print(f"工号格式正确: {'是' if valid_staff_ids else '否'}")
            print(f"操作类型有效: {'是' if valid_operations else '否'}")
            print(f"操作结果有效: {'是' if valid_results else '否'}")
            print(f"熟练度在有效范围: {'是' if valid_skills else '否'}")
            
            return valid_staff_ids and valid_operations and valid_results and valid_skills
            
        elif data_type == 'environment':
            # 验证传感器ID格式
            valid_sensor_ids = df['温湿度传感器ID'].str.match(r'^TEMP\d{3}$').all()
            # 验证温度范围
            valid_temps = ((df['温度'] >= 0) & (df['温度'] <= 50)).all()
            # 验证湿度范围
            valid_humidity = ((df['湿度'] >= 0) & (df['湿度'] <= 100)).all()
            # 验证PM2.5范围
            valid_pm25 = ((df['PM2.5'] >= 0) & (df['PM2.5'] <= 500)).all()
            
            print(f"传感器ID格式正确: {'是' if valid_sensor_ids else '否'}")
            print(f"温度在有效范围: {'是' if valid_temps else '否'}")
            print(f"湿度在有效范围: {'是' if valid_humidity else '否'}")
            print(f"PM2.5在有效范围: {'是' if valid_pm25 else '否'}")
            
            return valid_sensor_ids and valid_temps and valid_humidity and valid_pm25
        
        return True
        
    except Exception as e:
        print(f"验证筛选后数据时出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    test_messy_data_filtering() 