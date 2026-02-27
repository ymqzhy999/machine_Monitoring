import time
import random
from datetime import datetime, timedelta
from models import metrics_cache, get_device_metrics_cache, store_metrics
import pandas as pd
import sqlite3
import numpy as np
import os
import sys

# 从app.py导入日志控制设置
try:
    from app import VERBOSE_LOGGING, log_info
except ImportError:
    # 如果无法导入，使用默认设置
    VERBOSE_LOGGING = False
    def log_info(message, force=False):
        if VERBOSE_LOGGING or force:
            print(message)

# 这个文件只是作为中间层转发调用到 models.py
# 所有函数直接传递调用到对应的 models 函数

__all__ = ['get_latest_metrics', 'get_metrics_by_timerange', 'update_device_metrics', 'get_all_devices_latest_metrics', 'cleanup_cache', 'save_analyzer_to_excel']

def get_latest_metrics(device_id):
    """获取指定设备的最新指标"""
    # 检查缓存中是否存在该设备的数据
    cache_key = f"latest_metrics_{device_id}"
    now = datetime.now()
    
    for key, (timestamp, data) in metrics_cache.items():
        if key == cache_key:
            # 检查数据是否过期
            if (now - timestamp).total_seconds() < 3600:  # 1小时内的数据视为有效
                return data
            else:
                # 数据已过期，从缓存中删除
                del metrics_cache[key]
                break
    
    # 缓存中没有该设备的数据或数据已过期，生成新数据
    metrics = update_device_metrics(device_id)
    return metrics

def get_all_devices_latest_metrics():
    """获取所有设备的最新指标"""
    all_metrics = {}
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    
    for device_id in device_ids:
        metrics = get_latest_metrics(device_id)
        if metrics:
            all_metrics[device_id] = metrics
    
    return all_metrics

def get_metrics_by_timerange(device_id, days):
    """获取指定设备在指定天数内的历史指标"""
    cache_key = f"metrics_{device_id}_{days}"
    now = datetime.now()
    
    # 检查缓存
    for key, (timestamp, data) in metrics_cache.items():
        if key == cache_key:
            # 检查数据是否过期
            if (now - timestamp).total_seconds() < 3600:  # 1小时内的数据视为有效
                return data
            else:
                # 数据已过期，从缓存中删除
                del metrics_cache[key]
                break
    
    # 生成历史数据
    metrics = []
    end_date = datetime.now()
    
    for i in range(days):
        date = end_date - timedelta(days=i)
        
        # 生成该日期的OEE和指标数据
        daily_metrics = {
            'date': date.strftime('%Y-%m-%d'),
            'oee': random.uniform(60, 85),
            'availability': random.uniform(75, 95),
            'performance': random.uniform(80, 90),
            'quality': random.uniform(90, 98)
        }
        metrics.append(daily_metrics)
    
    # 存入缓存
    metrics_cache[cache_key] = (now, metrics)
    
    return metrics

def update_device_metrics(device_id, data=None):
    """更新设备指标"""
    # 如果提供了analyzer数据，使用analyzer中的数据计算OEE
    if data is not None and hasattr(data, 'equipment_data') and hasattr(data, 'material_data'):
        try:
            log_info(f"使用实际数据计算设备 {device_id} 的指标")
            
            # 过滤该设备的数据
            equipment_data = data.equipment_data[data.equipment_data['设备ID'] == device_id]
            if len(equipment_data) == 0:
                log_info(f"警告: 设备 {device_id} 没有设备数据记录，将使用默认数据")
                return update_device_metrics(device_id)  # 递归调用，但不传入data参数
            
            # 获取最近30天的数据
            recent_date = datetime.now() - timedelta(days=30)
            if '时间戳' in equipment_data.columns:
                # 确保时间戳列是datetime类型
                if equipment_data['时间戳'].dtype != 'datetime64[ns]':
                    try:
                        equipment_data['时间戳'] = pd.to_datetime(equipment_data['时间戳'])
                    except:
                        log_info(f"警告: 无法转换时间戳列为datetime类型")
                        pass
                
                # 过滤最近30天的数据
                recent_equipment_data = equipment_data[equipment_data['时间戳'] >= recent_date]
                if len(recent_equipment_data) == 0:
                    log_info(f"警告: 设备 {device_id} 在最近30天内没有数据记录，将使用所有可用数据")
                    recent_equipment_data = equipment_data
            else:
                recent_equipment_data = equipment_data
                
            log_info(f"使用最近30天的 {len(recent_equipment_data)} 条设备数据")
                
            # 计算可用性 (运行状态记录数 / 总记录数)
            if '设备状态' in recent_equipment_data.columns:
                running_records = len(recent_equipment_data[recent_equipment_data['设备状态'] == '运行中'])
                total_records = len(recent_equipment_data)
                availability = (running_records / total_records * 100) if total_records > 0 else 80.0  # 默认80%
                log_info(f"计算可用性: 运行记录={running_records}, 总记录={total_records}, 可用性={availability:.1f}%")
            else:
                availability = 80.0  # 默认值
                log_info(f"警告: 设备状态列不存在，使用默认可用性: {availability}%")
            
            # 获取该设备相关的物料数据
            if hasattr(data, 'material_data') and len(data.material_data) > 0:
                # 确保物料数据中有物料编号列
                if '物料编号' in data.material_data.columns:
                    material_data = data.material_data[data.material_data['物料编号'] == device_id]
                    log_info(f"找到 {len(material_data)} 条与设备 {device_id} 相关的物料数据")
                    
                    if len(material_data) > 0:
                        # 计算质量 (合格产品 / 总产品)
                        if '产品数量' in material_data.columns and '合格产品数量' in material_data.columns:
                            total_products = material_data['产品数量'].sum()
                            qualified_products = material_data['合格产品数量'].sum()
                            # 确保合格产品数不超过总产品数
                            qualified_products = min(qualified_products, total_products)
                            quality = (qualified_products / total_products * 100) if total_products > 0 else 95.0
                            log_info(f"计算质量: 总产品={total_products}, 合格产品={qualified_products}, 质量={quality:.1f}%")
                        else:
                            quality = 95.0  # 默认95%
                            log_info(f"警告: 产品数量或合格产品数量列不存在，使用默认质量: {quality}%")
                    else:
                        quality = 95.0  # 默认95%
                        log_info(f"警告: 没有找到与设备 {device_id} 相关的物料数据，使用默认质量: {quality}%")
                else:
                    quality = 95.0  # 默认95%
                    log_info(f"警告: 物料编号列不存在，使用默认质量: {quality}%")
            else:
                quality = 95.0  # 默认95%
                log_info(f"警告: 无法访问物料数据，使用默认质量: {quality}%")
            
            # 计算性能 (基于平均运行时间)
            if '总运行时间' in recent_equipment_data.columns:
                running_data = recent_equipment_data[recent_equipment_data['设备状态'] == '运行中']
                if len(running_data) > 0:
                    # 不再使用固定值，而是基于设备运行时间数据计算性能
                    avg_runtime = running_data['总运行时间'].mean()
                    max_runtime = running_data['总运行时间'].max()
                    min_runtime = running_data['总运行时间'].min() if min_runtime > 0 else max_runtime * 0.5
                    ideal_runtime = min_runtime * 0.9  # 理想运行时间为最短时间的90%
                    
                    # 性能 = 理想运行时间/平均运行时间
                    performance_ratio = ideal_runtime / avg_runtime if avg_runtime > 0 else 0.85
                    # 将比率转换为百分比，并限制在合理范围内
                    performance = min(98.0, max(75.0, performance_ratio * 100))
                    log_info(f"计算性能: 平均运行时间={avg_runtime:.2f}, 理想运行时间={ideal_runtime:.2f}, 性能={performance:.1f}%")
                else:
                    performance = 85.0  # 默认85%
                    log_info(f"警告: 没有运行状态记录，使用默认性能: {performance}%")
            else:
                performance = 85.0  # 默认85%
                log_info(f"警告: 总运行时间列不存在，使用默认性能: {performance}%")
            
            # 计算OEE
            oee = (availability * performance * quality) / 10000
            log_info(f"计算OEE: ({availability:.1f} * {performance:.1f} * {quality:.1f}) / 10000 = {oee:.1f}%")
            
            # 计算TEEP (总体设备效能) = OEE * 设备利用率
            # 设备利用率 = 计划生产时间 / 日历时间，这里简化为可用性的95%
            utilization = min(98.0, availability * 0.95)
            teep = (oee * utilization) / 100
            log_info(f"计算TEEP: ({oee:.1f}% * {utilization:.1f}%) / 100 = {teep:.1f}%")
            
            # 组织结果
            result = {
                'device_id': device_id,
                'oee': round(oee, 1),
                'availability': round(availability, 1),
                'performance': round(performance, 1),
                'quality': round(quality, 1),
                'teep': round(teep, 1),
                'utilization': round(utilization, 1),
                'downtime': int((100 - availability) * 0.6),  # 近似计算停机时间（分钟）
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'actual'
            }
            
            # 缓存结果
            cache_key = f"latest_metrics_{device_id}"
            metrics_cache[cache_key] = (datetime.now(), result)
            
            log_info(f"已更新设备 {device_id} 指标: OEE={oee:.1f}%, 可用性={availability:.1f}%, 性能={performance}%, 质量={quality:.1f}%")
            
            return result
        except Exception as e:
            log_info(f"计算设备 {device_id} 指标时出错: {str(e)}")
            import traceback
            log_info(traceback.format_exc())
            # 发生错误时使用模拟数据
            return generate_mock_metrics_for_device(device_id)
    else:
        # 如果没有提供数据，使用模拟数据
        return generate_mock_metrics_for_device(device_id)

def cleanup_cache():
    """清理过期的缓存数据"""
    now = datetime.now()
    expired_keys = []
    for key, (timestamp, _) in metrics_cache.items():
        if (now - timestamp).total_seconds() > 3600:  # 1小时过期
            expired_keys.append(key)
    
    for key in expired_keys:
        del metrics_cache[key]
    
    if expired_keys:
        log_info(f"已清理 {len(expired_keys)} 个过期缓存项")

def generate_mock_metrics_for_device(device_id):
    """为单个设备生成模拟指标数据"""
    log_info(f"使用模拟数据生成设备 {device_id} 的指标")
    
    # 生成合理范围内的随机值，确保OEE计算正确
    availability = round(random.uniform(80, 97), 1)
    performance = round(random.uniform(82, 94), 1)
    quality = round(random.uniform(94, 99), 1)
    
    # 计算OEE
    oee = round((availability * performance * quality) / 10000, 1)
    
    # 组织结果
    result = {
        'device_id': device_id,
        'oee': oee,
        'availability': availability,
        'performance': performance,
        'quality': quality,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_source': 'mock'
    }
    
    # 缓存结果
    cache_key = f"latest_metrics_{device_id}"
    metrics_cache[cache_key] = (datetime.now(), result)
    
    log_info(f"已为设备 {device_id} 生成模拟指标: OEE={oee}%, 可用性={availability}%, 性能={performance}%, 质量={quality}%")
    
    return result

def generate_mock_metrics():
    """生成所有设备的模拟指标数据"""
    metrics = []
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    
    for device_id in device_ids:
        metrics.append(generate_mock_metrics_for_device(device_id))
    
    return metrics

def save_analyzer_to_excel(analyzer, filename='test_data.xlsx', is_sample=False):
    """将分析器数据保存到Excel文件，确保持久化"""
    try:
        # 确保data目录存在
        data_dir = os.path.join('data')
        os.makedirs(data_dir, exist_ok=True)
        
        # 确定文件路径，如果是样例数据则添加时间戳
        if is_sample:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"sample_data_{timestamp_str}.xlsx"
        else:
            output_filename = filename
            
        output_path = os.path.join(data_dir, output_filename)
        
        log_info(f"正在保存数据到 {output_path}...", force=True)
        
        # 保存数据到Excel
        with pd.ExcelWriter(output_path) as writer:
            analyzer.equipment_data.to_excel(writer, sheet_name='设备数据', index=False)
            analyzer.operation_data.to_excel(writer, sheet_name='人员操作数据', index=False)
            analyzer.material_data.to_excel(writer, sheet_name='物料数据', index=False)
            analyzer.environment_data.to_excel(writer, sheet_name='环境数据', index=False)
        
        log_info(f"数据已成功保存到 {output_path}", force=True)
        
        # 如果是样例数据，同时保存一个test_data.xlsx副本
        if is_sample and filename != 'test_data.xlsx':
            default_path = os.path.join(data_dir, 'test_data.xlsx')
            with pd.ExcelWriter(default_path) as writer:
                analyzer.equipment_data.to_excel(writer, sheet_name='设备数据', index=False)
                analyzer.operation_data.to_excel(writer, sheet_name='人员操作数据', index=False)
                analyzer.material_data.to_excel(writer, sheet_name='物料数据', index=False)
                analyzer.environment_data.to_excel(writer, sheet_name='环境数据', index=False)
            log_info(f"同时更新了系统默认数据文件", force=True)
            
        return True, output_path
    except Exception as e:
        log_info(f"保存数据到Excel时出错: {str(e)}", force=True)
        import traceback
        log_info(traceback.format_exc(), force=True)
        return False, str(e) 