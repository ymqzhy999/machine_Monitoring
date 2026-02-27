import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import string
import os

"""
生成混乱测试数据脚本

此脚本将生成包含各种问题的混乱数据，用于测试系统的数据清洗和字段筛选能力：
1. 缺失值 (NaN, None, 空字符串)
2. 格式错误的日期和数字
3. 异常值（超出正常范围的数据）
4. 重复记录
5. 额外的无关字段
6. 拼写错误的列名
7. 数据类型混合（字符串与数字混合）
8. 大小写不一致
"""

def random_string(length=10):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_messy_equipment_data(rows=500):
    """生成混乱的设备数据"""
    # 基本数据
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    statuses = ['运行中', '停机', '维护', '故障', '待机']
    warning_statuses = ['正常', '轻微', '严重']
    
    data = []
    
    # 生成基础数据
    for i in range(rows):
        # 有10%的概率设备ID为错误格式
        if random.random() < 0.1:
            device_id = f"错误格式_{random_string(3)}"
        else:
            device_id = random.choice(device_ids)
            
        # 有5%的概率时间戳为错误格式
        if random.random() < 0.05:
            timestamp = f"无效日期_{random_string(8)}"
        else:
            days_ago = random.randint(0, 30)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            timestamp = (datetime.now() - timedelta(days=days_ago, hours=hours, minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 有20%的概率状态为NaN或其他错误值
        if random.random() < 0.2:
            if random.random() < 0.5:
                status = np.nan
            else:
                status = random.choice(['未知', '测试中', 123])  # 包括数字作为状态
        else:
            status = random.choice(statuses)
        
        # 有30%的概率运行时间为异常值或NaN
        if random.random() < 0.3:
            if random.random() < 0.5:
                runtime = np.nan
            elif random.random() < 0.3:
                runtime = -random.uniform(1, 100)  # 负数运行时间
            else:
                runtime = random.uniform(1000, 10000)  # 异常大的运行时间
        else:
            runtime = random.uniform(10, 150)
        
        # 有15%的概率故障次数为字符串或异常值
        if random.random() < 0.15:
            if random.random() < 0.5:
                failures = str(random.randint(0, 10))  # 字符串形式的数字
            else:
                failures = random.randint(100, 1000)  # 异常大的故障次数
        else:
            failures = random.randint(0, 5)
        
        # 有25%的概率预警状态为无效值
        if random.random() < 0.25:
            if random.random() < 0.5:
                warning = ""  # 空字符串
            else:
                warning = random.choice(["警告", "未定义", 404])  # 包括数字错误码
        else:
            warning = random.choice(warning_statuses)
        
        # 添加普通记录
        record = {
            '设备ID': device_id,
            '时间戳': timestamp,
            '设备状态': status,
            '总运行时间': runtime,
            '故障次数': failures,
            '预警状态': warning
        }
        
        # 有10%的概率添加额外的无关字段
        if random.random() < 0.1:
            record['无关字段1'] = random_string()
            record['无关字段2'] = random.randint(1, 100)
            record['备注'] = f"测试记录 #{i}"
        
        # 有5%的概率字段名称拼写错误
        if random.random() < 0.05:
            # 创建新记录以修改键名
            new_record = {}
            for key, value in record.items():
                if key == '设备ID' and random.random() < 0.5:
                    new_record['设备Id'] = value  # 小写的d
                elif key == '总运行时间' and random.random() < 0.5:
                    new_record['总运作时间'] = value  # 用"运作"代替"运行"
                else:
                    new_record[key] = value
            record = new_record
        
        data.append(record)
    
    # 添加一些完全重复的记录
    duplicates = random.sample(data, int(rows * 0.15))
    data.extend(duplicates)
    
    # 添加完全缺失的记录
    for _ in range(int(rows * 0.05)):
        empty_record = {
            '设备ID': np.nan,
            '时间戳': np.nan,
            '设备状态': np.nan,
            '总运行时间': np.nan,
            '故障次数': np.nan,
            '预警状态': np.nan
        }
        data.append(empty_record)
    
    return pd.DataFrame(data)

def generate_messy_material_data(rows=300):
    """生成混乱的物料数据"""
    # 基本数据
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    
    data = []
    
    # 生成基础数据
    for i in range(rows):
        # 有10%的概率物料编号为错误格式
        if random.random() < 0.1:
            material_id = f"错误_{random_string(3)}"
        else:
            material_id = random.choice(device_ids)
            
        # 有5%的概率日期为错误格式
        if random.random() < 0.05:
            date = f"无效日期_{random_string(5)}"
        else:
            days_ago = random.randint(0, 30)
            date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # 生成产品数量，有20%的概率数量不合理
        if random.random() < 0.2:
            if random.random() < 0.5:
                total = random.randint(-100, 0)  # 负数产品数量
            else:
                total = random.randint(10000, 100000)  # 异常大的产品数量
        else:
            total = random.randint(50, 200)
        
        # 生成合格品数量，有25%的概率比例不合理
        if random.random() < 0.25:
            if random.random() < 0.7:
                qualified = random.randint(total + 1, total + 100) if total >= 0 else random.randint(1, 100)  # 合格品超过总数
            else:
                qualified = random.randint(-50, 0) if total >= 0 else random.randint(-100, -50)  # 负数合格品
        else:
            # 确保不会出现空范围错误
            if total > 0:
                min_qualified = max(0, int(total * 0.8))
                qualified = random.randint(min_qualified, total)  # 正常范围内的合格品
            else:
                qualified = 0  # 当总数为负数或0时，合格品为0
        
        # 添加普通记录
        record = {
            '物料编号': material_id,
            '日期': date,
            '产品数量': total,
            '合格产品数量': qualified
        }
        
        # 有15%的概率添加额外的无关字段
        if random.random() < 0.15:
            record['批次号'] = f"BATCH-{random.randint(1000, 9999)}"
            record['操作员'] = f"OP-{random.randint(10, 99)}"
            record['生产线'] = random.choice(['A', 'B', 'C'])
        
        # 有5%的概率字段名称拼写错误
        if random.random() < 0.05:
            # 创建新记录以修改键名
            new_record = {}
            for key, value in record.items():
                if key == '合格产品数量' and random.random() < 0.5:
                    new_record['合格品数量'] = value  # 省略"产品"二字
                elif key == '产品数量' and random.random() < 0.5:
                    new_record['产量'] = value  # 使用不同表达
                else:
                    new_record[key] = value
            record = new_record
        
        data.append(record)
    
    # 添加一些完全重复的记录
    duplicates = random.sample(data, int(rows * 0.1))
    data.extend(duplicates)
    
    # 添加完全缺失的记录
    for _ in range(int(rows * 0.05)):
        empty_record = {
            '物料编号': np.nan,
            '日期': np.nan,
            '产品数量': np.nan,
            '合格产品数量': np.nan
        }
        data.append(empty_record)
    
    return pd.DataFrame(data)

def generate_messy_operation_data(rows=400):
    """生成混乱的人员操作数据"""
    # 基本数据
    device_ids = ['CNC001', 'CNC002', 'CNC003', 'CNC004', 'CNC005']
    staff_ids = ['W001', 'W002', 'W003', 'W004', 'W005']
    operation_types = ['上料', '下料', '维护', '质检']
    results = ['正常', '异常']
    
    data = []
    
    # 生成基础数据
    for i in range(rows):
        # 有10%的概率工号为错误格式
        if random.random() < 0.1:
            staff_id = f"错误_{random_string(3)}"
        else:
            staff_id = random.choice(staff_ids)
            
        # 有5%的概率时间戳为错误格式
        if random.random() < 0.05:
            timestamp = f"无效时间_{random_string(5)}"
        else:
            days_ago = random.randint(0, 30)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            timestamp = (datetime.now() - timedelta(days=days_ago, hours=hours, minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 有10%的概率设备ID为错误
        if random.random() < 0.1:
            device_id = f"未知设备_{random.randint(1, 100)}"
        else:
            device_id = random.choice(device_ids)
        
        # 有15%的概率操作类型错误
        if random.random() < 0.15:
            if random.random() < 0.5:
                operation_type = ""  # 空字符串
            else:
                operation_type = random.choice(["未知操作", "测试", 123])  # 包括数字
        else:
            operation_type = random.choice(operation_types)
        
        # 有20%的概率操作时长异常
        if random.random() < 0.2:
            if random.random() < 0.5:
                duration = random.uniform(-5, 0)  # 负数时长
            else:
                duration = random.uniform(10, 100)  # 异常长的时长
        else:
            duration = random.uniform(0.5, 2.5)
        
        # 有10%的概率操作结果错误
        if random.random() < 0.1:
            if random.random() < 0.5:
                result = np.nan  # 缺失值
            else:
                result = random.choice([0, 1, "完成", "失败"])  # 非标准结果
        else:
            result = random.choice(results)
        
        # 有25%的概率熟练度异常
        if random.random() < 0.25:
            if random.random() < 0.3:
                skill = random.uniform(-0.5, 0)  # 负数熟练度
            elif random.random() < 0.3:
                skill = random.uniform(1.5, 10)  # 超过1的熟练度
            else:
                skill = str(random.uniform(0.6, 1.0))  # 字符串熟练度
        else:
            skill = random.uniform(0.6, 1.0)
        
        # 添加普通记录
        record = {
            '工号': staff_id,
            '时间戳': timestamp,
            '设备ID': device_id,
            '操作类型': operation_type,
            '操作时长': duration,
            '操作结果': result,
            '熟练度': skill
        }
        
        # 有12%的概率添加额外的无关字段
        if random.random() < 0.12:
            record['操作编码'] = f"OP-{random.randint(1000, 9999)}"
            record['备注'] = f"测试备注 #{i}"
            record['温度'] = random.uniform(18, 30)
        
        data.append(record)
    
    # 添加一些完全重复的记录
    duplicates = random.sample(data, int(rows * 0.08))
    data.extend(duplicates)
    
    # 添加完全缺失的记录
    for _ in range(int(rows * 0.03)):
        empty_record = {
            '工号': np.nan,
            '时间戳': np.nan,
            '设备ID': np.nan,
            '操作类型': np.nan,
            '操作时长': np.nan,
            '操作结果': np.nan,
            '熟练度': np.nan
        }
        data.append(empty_record)
    
    return pd.DataFrame(data)

def generate_messy_environment_data(rows=200):
    """生成混乱的环境数据"""
    # 基本数据
    sensor_ids = ['TEMP001', 'TEMP002', 'TEMP003']
    locations = ['车间A区', '车间B区', '车间C区']
    warning_statuses = ['正常', '轻微', '严重']
    
    data = []
    
    # 生成基础数据
    for i in range(rows):
        # 有10%的概率传感器ID错误
        if random.random() < 0.1:
            sensor_id = f"错误传感器_{random.randint(1, 100)}"
        else:
            sensor_id = random.choice(sensor_ids)
            
        # 有5%的概率时间戳为错误格式
        if random.random() < 0.05:
            timestamp = f"无效时间_{random_string(5)}"
        else:
            days_ago = random.randint(0, 30)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            timestamp = (datetime.now() - timedelta(days=days_ago, hours=hours, minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 有15%的概率温度异常
        if random.random() < 0.15:
            if random.random() < 0.5:
                temperature = random.uniform(-20, 0)  # 过低温度
            else:
                temperature = random.uniform(50, 100)  # 过高温度
        else:
            temperature = random.uniform(18, 32)
        
        # 有20%的概率湿度异常
        if random.random() < 0.2:
            if random.random() < 0.3:
                humidity = random.uniform(-10, 0)  # 负数湿度
            elif random.random() < 0.4:
                humidity = random.uniform(100, 150)  # 超100%湿度
            else:
                humidity = str(random.uniform(40, 70))  # 字符串湿度
        else:
            humidity = random.uniform(40, 70)
        
        # 有25%的概率PM2.5异常
        if random.random() < 0.25:
            if random.random() < 0.5:
                pm25 = random.uniform(-50, 0)  # 负数PM2.5
            else:
                pm25 = random.uniform(1000, 5000)  # 异常高PM2.5
        else:
            pm25 = random.uniform(10, 150)
        
        # 有10%的概率位置错误
        if random.random() < 0.1:
            if random.random() < 0.5:
                location = np.nan  # 缺失位置
            else:
                location = random.choice(["未知", 123, "测试区域"])
        else:
            location = random.choice(locations)
        
        # 有15%的概率预警状态错误
        if random.random() < 0.15:
            if random.random() < 0.5:
                warning = ""  # 空字符串
            else:
                warning = random.choice(["高危", "低危", 0, 1, 2])
        else:
            warning = random.choice(warning_statuses)
        
        # 添加普通记录
        record = {
            '温湿度传感器ID': sensor_id,
            '时间戳': timestamp,
            '温度': temperature,
            '湿度': humidity,
            'PM2.5': pm25,
            '位置': location,
            '预警状态': warning
        }
        
        # 有10%的概率添加额外的无关字段
        if random.random() < 0.1:
            record['噪音分贝'] = random.uniform(40, 90)
            record['气压'] = random.uniform(990, 1020)
            record['检测批次'] = f"BATCH-{random.randint(1000, 9999)}"
        
        data.append(record)
    
    # 添加一些完全重复的记录
    duplicates = random.sample(data, int(rows * 0.1))
    data.extend(duplicates)
    
    # 添加完全缺失的记录
    for _ in range(int(rows * 0.05)):
        empty_record = {
            '温湿度传感器ID': np.nan,
            '时间戳': np.nan,
            '温度': np.nan,
            '湿度': np.nan,
            'PM2.5': np.nan,
            '位置': np.nan,
            '预警状态': np.nan
        }
        data.append(empty_record)
    
    return pd.DataFrame(data)

def main():
    """主函数，生成所有类型的混乱数据并导出到Excel文件"""
    # 创建输出目录
    os.makedirs('test_data', exist_ok=True)
    
    print("正在生成混乱的测试数据...")
    
    # 生成各类混乱数据
    equipment_data = generate_messy_equipment_data()
    material_data = generate_messy_material_data()
    operation_data = generate_messy_operation_data()
    environment_data = generate_messy_environment_data()
    
    # 将所有数据保存到一个Excel文件的不同sheet中
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = f'test_data/messy_test_data_{timestamp}.xlsx'
    
    with pd.ExcelWriter(excel_path) as writer:
        equipment_data.to_excel(writer, sheet_name='设备数据', index=False)
        material_data.to_excel(writer, sheet_name='物料数据', index=False)
        operation_data.to_excel(writer, sheet_name='人员操作数据', index=False)
        environment_data.to_excel(writer, sheet_name='环境数据', index=False)
    
    print(f"混乱测试数据已生成，保存到：{excel_path}")
    print(f"设备数据：{len(equipment_data)}行，物料数据：{len(material_data)}行")
    print(f"人员操作数据：{len(operation_data)}行，环境数据：{len(environment_data)}行")
    print(f"\n您可以将该Excel文件导入系统，测试数据清洗和字段筛选功能")

if __name__ == "__main__":
    main() 