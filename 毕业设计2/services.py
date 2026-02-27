from models import Session, DeviceMetrics
from datetime import datetime, timedelta
import random
from functools import lru_cache
import time

# 添加内存缓存
metrics_cache = {}
CACHE_TIMEOUT = 60  # 缓存60秒

def get_cache(key):
    if key in metrics_cache:
        data, timestamp = metrics_cache[key]
        if time.time() - timestamp < CACHE_TIMEOUT:
            return data
    return None

def set_cache(key, data):
    metrics_cache[key] = (data, time.time())

@lru_cache(maxsize=32)
def get_latest_metrics(device_id=None):
    # 检查缓存
    cache_key = f'latest_metrics_{device_id}'
    cached_data = get_cache(cache_key)
    if cached_data:
        return cached_data

    session = Session()
    try:
        query = session.query(DeviceMetrics)
        if device_id:
            query = query.filter(DeviceMetrics.device_id == device_id)
        latest_metrics = query.order_by(DeviceMetrics.timestamp.desc()).first()
        result = latest_metrics.to_dict() if latest_metrics else None
        
        # 更新缓存
        if result:
            set_cache(cache_key, result)
        return result
    finally:
        session.close()

@lru_cache(maxsize=32)
def get_metrics_by_timerange(device_id, days):
    # 检查缓存
    cache_key = f'metrics_range_{device_id}_{days}'
    cached_data = get_cache(cache_key)
    if cached_data:
        return cached_data

    session = Session()
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        query = session.query(DeviceMetrics)
        if device_id:
            query = query.filter(DeviceMetrics.device_id == device_id)
        metrics = query.filter(DeviceMetrics.timestamp >= start_date).all()
        result = [metric.to_dict() for metric in metrics]
        
        # 更新缓存
        set_cache(cache_key, result)
        return result
    finally:
        session.close()

def update_device_metrics(device_id):
    """
    更新设备指标（示例函数）
    实际生产环境中应该从真实数据源获取数据
    """
    session = Session()
    try:
        # 模拟数据，实际应从设备或其他数据源获取
        new_metric = DeviceMetrics(
            device_id=device_id,
            availability=random.uniform(0.85, 0.98),
            performance=random.uniform(0.80, 0.95),
            quality=random.uniform(0.90, 0.99),
            oee=random.uniform(0.75, 0.90),
            teep=random.uniform(0.70, 0.85),
            utilization=random.uniform(0.75, 0.95),
            downtime=random.uniform(0, 120)  # 分钟
        )
        session.add(new_metric)
        session.commit()
        
        # 更新缓存
        result = new_metric.to_dict()
        set_cache(f'latest_metrics_{device_id}', result)
        return result
    finally:
        session.close()

def get_all_devices_latest_metrics():
    """
    获取所有设备的最新指标
    """
    # 检查缓存
    cache_key = 'all_devices_latest'
    cached_data = get_cache(cache_key)
    if cached_data:
        return cached_data

    session = Session()
    try:
        # 使用子查询方式替代from_self()
        subquery = session.query(
            DeviceMetrics.device_id,
            DeviceMetrics.id,
            DeviceMetrics.timestamp
        ).order_by(
            DeviceMetrics.device_id,
            DeviceMetrics.timestamp.desc()
        ).distinct(DeviceMetrics.device_id).subquery()
        
        latest_metrics = session.query(DeviceMetrics).join(
            subquery,
            DeviceMetrics.id == subquery.c.id
        ).all()

        # 如果查询失败，返回空列表并记录错误
        if latest_metrics is None:
            return []

        result = [metric.to_dict() for metric in latest_metrics]
        
        # 更新缓存
        set_cache(cache_key, result)
        return result
    except Exception as e:
        print(f"Error getting latest metrics: {e}")
        return []
    finally:
        session.close()

# 清理过期缓存
def cleanup_cache():
    current_time = time.time()
    expired_keys = [
        key for key, (_, timestamp) in metrics_cache.items()
        if current_time - timestamp >= CACHE_TIMEOUT
    ]
    for key in expired_keys:
        metrics_cache.pop(key, None) 