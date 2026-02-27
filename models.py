import os
from datetime import datetime, timedelta
import random
import time

# 内存缓存，用于存储设备指标数据
metrics_cache = {}

def init_db():
    """初始化内存缓存"""
    # 清空缓存
    global metrics_cache
    metrics_cache = {}
    
    # 预先生成一些设备数据
    from services import update_device_metrics
    
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    for device_id in device_ids:
        update_device_metrics(device_id)

def get_device_metrics_cache(device_id):
    """获取设备的指标缓存"""
    if device_id not in metrics_cache:
        metrics_cache[device_id] = {
            'latest': {},
            'history': []
        }
    return metrics_cache[device_id]

def store_metrics(device_id, metrics):
    """存储设备指标到内存缓存"""
    cache = get_device_metrics_cache(device_id)
    
    # 更新最新指标
    cache['latest'] = metrics.copy()
    
    # 添加到历史记录
    metrics['timestamp'] = datetime.now()
    cache['history'].append(metrics.copy())
    
    # 限制历史记录大小
    if len(cache['history']) > 100:
        cache['history'] = cache['history'][-100:]

def get_latest_metrics(device_id=None):
    """获取设备的最新指标"""
    if device_id:
        if device_id in metrics_cache:
            return metrics_cache[device_id]
        return None
    else:
        # 返回所有设备指标
        return list(metrics_cache.values())

def get_all_devices_latest_metrics():
    """获取所有设备的最新指标"""
    return list(metrics_cache.values())

def get_metrics_by_timerange(device_id, days):
    """获取指定设备在一定天数内的指标"""
    # 简化版，仅返回当前缓存的值
    if device_id in metrics_cache:
        return {"history": [metrics_cache[device_id]]}
    return {"history": []}

def update_device_metrics(device_id, data=None):
    """更新设备指标"""
    # 如果没有数据，生成一些模拟数据
    if data is None:
        # 根据设备ID设置不同的基准值，保证每个设备有差异
        device_index = int(device_id.replace('CNC', '')) if device_id.replace('CNC', '').isdigit() else 1
        base_availability = 85 + (device_index % 5) * 2
        base_performance = 90 - (device_index % 4) * 3
        base_quality = 96 + (device_index % 4)
        
        availability = round(min(98, max(78, base_availability + random.uniform(-2, 2))), 1)
        performance = round(min(95, max(80, base_performance + random.uniform(-3, 3))), 1)
        quality = round(min(99, max(95, base_quality + random.uniform(-1, 1))), 1)
        oee = round((availability * performance * quality) / 10000, 1)
        
        data = {
            "device_id": device_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": oee,
            "teep": round(oee * 0.9, 1),  # TEEP通常稍低于OEE
            "utilization": round(availability * 0.95, 1),  # 利用率略低于可用性
            "downtime": round(random.uniform(10, 100) * (100 - availability) / 20),  # 停机时间与可用性成反比
            "status": random.choice(["运行中", "停机", "维护"]) if random.random() < 0.2 else "运行中"
        }
    
    metrics_cache[device_id] = data
    return data

def cleanup_cache():
    """清理缓存（实际什么都不做）"""
    return True 