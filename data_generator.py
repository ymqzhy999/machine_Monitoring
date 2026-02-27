import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

class ProductionDataGenerator:
    def __init__(self):
        # 基础参数设置
        self.start_date = datetime(2025, 4, 6)
        self.end_date = datetime(2025, 5, 6)
        self.equipment_count = 5
        self.worker_count = 10
        self.material_types = 3
        self.sensor_count = 8
        
        # 预警阈值设置
        self.thresholds = {
            '温度': {'min': 18, 'max': 28, 'critical_min': 15, 'critical_max': 35},
            '湿度': {'min': 40, 'max': 70, 'critical_min': 30, 'critical_max': 80},
            'PM2.5': {'min': 0, 'max': 35, 'critical_min': 0, 'critical_max': 75},
            '振动': {'min': 0.1, 'max': 0.5, 'critical_min': 0, 'critical_max': 1.0},
            '设备温度': {'min': 20, 'max': 35, 'critical_min': 15, 'critical_max': 45},
            '质量合格率': {'min': 0.95, 'warning': 0.92, 'critical': 0.90},
            '设备利用率': {'min': 0.85, 'warning': 0.80, 'critical': 0.75}
        }

    def generate_equipment_data(self):
        """生成设备数据，包含预警所需的各种指标"""
        data = []
        current_date = self.start_date
        
        # 为每台设备生成基础状态序列
        equipment_states = {}
        for equipment_id in range(1, self.equipment_count + 1):
            equipment_states[f'CNC{str(equipment_id).zfill(3)}'] = {
                'maintenance_due': current_date + timedelta(days=random.randint(10, 25)),
                'last_maintenance': current_date - timedelta(days=random.randint(5, 15)),
                'total_runtime': 0,
                'fault_count': 0
            }
        
        while current_date <= self.end_date:
            for equipment_id in range(1, self.equipment_count + 1):
                equip_id = f'CNC{str(equipment_id).zfill(3)}'
                state_info = equipment_states[equip_id]
                
                # 生成8小时班次的数据
                for hour in range(8):
                    # 基础运行数据
                    is_maintenance_due = current_date >= state_info['maintenance_due']
                    is_fault = random.random() < (0.05 if not is_maintenance_due else 0.15)  # 维护到期增加故障概率
                    
                    # 设备状态判断
                    if is_maintenance_due and random.random() < 0.3:
                        status = '待维护'
                    elif is_fault:
                        status = '故障'
                        state_info['fault_count'] += 1
                    else:
                        status = np.random.choice(['运行中', '待机'], p=[0.85, 0.15])
                    
                    # 生成传感器数据（包含一些异常值）
                    vibration = round(np.random.uniform(0.1, 0.5), 2)
                    temperature = round(np.random.uniform(20, 35), 1)
                    if is_fault or is_maintenance_due:
                        # 故障或需要维护时，增加异常值的概率
                        if random.random() < 0.4:
                            vibration *= 2
                        if random.random() < 0.4:
                            temperature *= 1.3
                    
                    # 计算运行时间
                    hour_runtime = 1.0 if status == '运行中' else 0
                    fault_duration = 1.0 if status == '故障' else 0
                    state_info['total_runtime'] += hour_runtime
                    
                    # 生成记录
                    record = {
                        '时间戳': current_date + timedelta(hours=hour),
                        '设备ID': equip_id,
                        '设备状态': status,
                        '总运行时间': state_info['total_runtime'],
                        '故障持续时间': fault_duration,
                        '故障次数': state_info['fault_count'],
                        '振动值': vibration,
                        '温度': temperature,
                        '维护到期时间': state_info['maintenance_due'],
                        '上次维护时间': state_info['last_maintenance'],
                        '预警状态': '正常'
                    }
                    
                    # 设置预警状态
                    warnings = []
                    if vibration > self.thresholds['振动']['max']:
                        warnings.append('振动异常')
                    if temperature > self.thresholds['设备温度']['max']:
                        warnings.append('温度异常')
                    if is_maintenance_due:
                        warnings.append('需要维护')
                    if state_info['fault_count'] > 5:  # 故障次数过多
                        warnings.append('故障频发')
                    
                    if warnings:
                        record['预警状态'] = '|'.join(warnings)
                    
                    data.append(record)
            
            current_date += timedelta(hours=8)  # 前进8小时
        
        return pd.DataFrame(data)

    def generate_operation_data(self):
        """生成人员操作数据，包含效率和质量指标"""
        data = []
        current_date = self.start_date
        
        # 为每个工人生成基础属性
        worker_profiles = {}
        for worker_id in range(1, self.worker_count + 1):
            worker_profiles[f'W{str(worker_id).zfill(3)}'] = {
                'base_skill': random.uniform(0.7, 0.95),  # 基础熟练度
                'error_rate': random.uniform(0.01, 0.05),  # 基础错误率
                'fatigue_factor': random.uniform(0.1, 0.3),  # 疲劳影响因子
                'training_date': current_date - timedelta(days=random.randint(30, 180))  # 上次培训日期
            }
        
        while current_date <= self.end_date:
            for worker_id in range(1, self.worker_count + 1):
                worker_code = f'W{str(worker_id).zfill(3)}'
                profile = worker_profiles[worker_code]
                
                # 每班次2-4条记录
                for _ in range(np.random.randint(2, 5)):
                    # 计算当前班次的时间点
                    operation_time = current_date + timedelta(hours=np.random.uniform(0, 8))
                    is_night_shift = operation_time.hour >= 18 or operation_time.hour < 6
                    
                    # 计算实际熟练度（考虑疲劳和夜班因素）
                    hours_worked = (operation_time - current_date).total_seconds() / 3600
                    fatigue_impact = 1 - (hours_worked * profile['fatigue_factor'] / 8)
                    night_shift_impact = 0.9 if is_night_shift else 1.0
                    actual_skill = profile['base_skill'] * fatigue_impact * night_shift_impact
                    
                    # 生成操作记录
                    operation_duration = round(np.random.uniform(0.5, 2.0), 2)  # 0.5-2小时
                    error_prob = profile['error_rate'] * (2 - fatigue_impact)  # 疲劳增加错误概率
                    
                    record = {
                        '时间戳': operation_time,
                        '工号': worker_code,
                        '班次': '夜班' if is_night_shift else '白班',
                        '操作时长': operation_duration,
                        '熟练度': round(actual_skill, 2),
                        '操作类型': np.random.choice(['上料', '下料', '质检', '调试', '维护']),
                        '操作结果': '异常' if random.random() < error_prob else '正常',
                        '上次培训日期': profile['training_date'],
                        '预警状态': '正常'
                    }
                    
                    # 设置预警状态
                    warnings = []
                    if actual_skill < 0.75:
                        warnings.append('效率低下')
                    if (current_date - profile['training_date']).days > 90:
                        warnings.append('需要培训')
                    if error_prob > 0.1:
                        warnings.append('错误率高')
                    
                    if warnings:
                        record['预警状态'] = '|'.join(warnings)
                    
                    data.append(record)
            
            current_date += timedelta(hours=8)
        
        return pd.DataFrame(data)

    def generate_material_data(self):
        """生成物料数据，包含质量和效率指标"""
        data = []
        current_date = self.start_date
        
        # 为每种物料生成基础属性
        material_profiles = {}
        for material_id in range(1, self.material_types + 1):
            material_profiles[f'M{str(material_id).zfill(3)}'] = {
                'base_quality_rate': random.uniform(0.92, 0.98),  # 基础合格率
                'standard_cycle_time': random.uniform(0.08, 0.15),  # 标准生产周期（小时/件）
                'batch_size': random.randint(80, 120)  # 标准批次大小
            }
        
        while current_date <= self.end_date:
            for material_id in range(1, self.material_types + 1):
                material_code = f'M{str(material_id).zfill(3)}'
                profile = material_profiles[material_code]
                
                # 生成批次数据
                batch_size = profile['batch_size'] + np.random.randint(-10, 10)
                actual_quality_rate = min(1.0, profile['base_quality_rate'] * 
                                       random.uniform(0.95, 1.05))  # 随机波动
                good_products = int(batch_size * actual_quality_rate)
                
                record = {
                    '日期': current_date,
                    '物料编号': material_code,
                    '批次号': f'B{current_date.strftime("%Y%m%d")}{str(material_id).zfill(2)}',
                    '物料投入量': batch_size + np.random.randint(5, 15),  # 考虑损耗
                    '物料使用量': batch_size,
                    '产品数量': batch_size,
                    '合格产品数量': good_products,
                    '标准周期时间': profile['standard_cycle_time'],
                    '实际周期时间': profile['standard_cycle_time'] * random.uniform(0.9, 1.2),
                    '预警状态': '正常'
                }
                
                # 设置预警状态
                warnings = []
                quality_rate = good_products / batch_size
                if quality_rate < self.thresholds['质量合格率']['critical']:
                    warnings.append('质量严重不合格')
                elif quality_rate < self.thresholds['质量合格率']['warning']:
                    warnings.append('质量不达标')
                
                if record['实际周期时间'] > profile['standard_cycle_time'] * 1.2:
                    warnings.append('生产效率低')
                
                if record['物料投入量'] - record['物料使用量'] > record['物料使用量'] * 0.1:
                    warnings.append('物料损耗大')
                
                if warnings:
                    record['预警状态'] = '|'.join(warnings)
                
                data.append(record)
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(data)

    def generate_environment_data(self):
        """生成环境数据，包含预警指标"""
        data = []
        current_date = self.start_date
        
        # 为每个传感器生成位置信息
        sensor_locations = {}
        for sensor_id in range(1, self.sensor_count + 1):
            sensor_locations[f'SENS{str(sensor_id).zfill(3)}'] = np.random.choice(
                ['生产区A', '生产区B', '仓库区', '质检区', '包装区']
            )
        
        while current_date <= self.end_date:
            for sensor_id in range(1, self.sensor_count + 1):
                sensor_code = f'SENS{str(sensor_id).zfill(3)}'
                location = sensor_locations[sensor_code]
                
                # 生成基础环境数据
                base_temp = 23 + np.sin(current_date.hour / 12 * np.pi) * 3  # 温度随时间变化
                base_humidity = 55 + np.cos(current_date.hour / 12 * np.pi) * 10  # 湿度随时间变化
                
                # 添加随机波动
                temperature = round(base_temp + np.random.uniform(-2, 2), 1)
                humidity = round(base_humidity + np.random.uniform(-5, 5), 1)
                pm25 = round(np.random.uniform(10, 50), 1)
                
                # 特定区域的环境特征
                if location == '仓库区':
                    humidity += 5  # 仓库湿度偏高
                elif location == '生产区A':
                    temperature += 3  # 生产区温度偏高
                
                record = {
                    '时间戳': current_date,
                    '温湿度传感器ID': sensor_code,
                    '位置': location,
                    '温度': temperature,
                    '湿度': humidity,
                    'PM2.5': pm25,
                    '预警状态': '正常'
                }
                
                # 设置预警状态
                warnings = []
                if temperature > self.thresholds['温度']['critical_max']:
                    warnings.append('温度严重偏高')
                elif temperature > self.thresholds['温度']['max']:
                    warnings.append('温度偏高')
                elif temperature < self.thresholds['温度']['critical_min']:
                    warnings.append('温度严重偏低')
                elif temperature < self.thresholds['温度']['min']:
                    warnings.append('温度偏低')
                
                if humidity > self.thresholds['湿度']['critical_max']:
                    warnings.append('湿度严重偏高')
                elif humidity > self.thresholds['湿度']['max']:
                    warnings.append('湿度偏高')
                elif humidity < self.thresholds['湿度']['critical_min']:
                    warnings.append('湿度严重偏低')
                elif humidity < self.thresholds['湿度']['min']:
                    warnings.append('湿度偏低')
                
                if pm25 > self.thresholds['PM2.5']['critical_max']:
                    warnings.append('PM2.5严重超标')
                elif pm25 > self.thresholds['PM2.5']['max']:
                    warnings.append('PM2.5超标')
                
                if warnings:
                    record['预警状态'] = '|'.join(warnings)
                
                data.append(record)
            
            current_date += timedelta(hours=1)
        
        return pd.DataFrame(data)

    def generate_all_data(self):
        """生成所有数据并保存"""
        try:
            # 确保data目录存在
            os.makedirs('data', exist_ok=True)
            
            # 生成各类数据
            print("正在生成设备数据...")
            equipment_data = self.generate_equipment_data()
            print("正在生成操作数据...")
            operation_data = self.generate_operation_data()
            print("正在生成物料数据...")
            material_data = self.generate_material_data()
            print("正在生成环境数据...")
            environment_data = self.generate_environment_data()
            
            # 保存为CSV文件
            print("\n保存数据文件...")
            equipment_data.to_csv('data/equipment_data.csv', index=False, encoding='utf-8-sig')
            operation_data.to_csv('data/operation_data.csv', index=False, encoding='utf-8-sig')
            material_data.to_csv('data/material_data.csv', index=False, encoding='utf-8-sig')
            environment_data.to_csv('data/environment_data.csv', index=False, encoding='utf-8-sig')
            
            # 同时保存为Excel文件（用于查看）
            with pd.ExcelWriter('data/test_data.xlsx') as writer:
                equipment_data.to_excel(writer, sheet_name='设备数据', index=False)
                operation_data.to_excel(writer, sheet_name='人员操作数据', index=False)
                material_data.to_excel(writer, sheet_name='物料数据', index=False)
                environment_data.to_excel(writer, sheet_name='环境数据', index=False)
            
            print("\n数据生成完成！")
            print(f"设备数据记录数: {len(equipment_data)}")
            print(f"操作数据记录数: {len(operation_data)}")
            print(f"物料数据记录数: {len(material_data)}")
            print(f"环境数据记录数: {len(environment_data)}")
            
            # 生成预警统计
            warnings = {
                '设备预警': len(equipment_data[equipment_data['预警状态'] != '正常']),
                '人员预警': len(operation_data[operation_data['预警状态'] != '正常']),
                '物料预警': len(material_data[material_data['预警状态'] != '正常']),
                '环境预警': len(environment_data[environment_data['预警状态'] != '正常'])
            }
            
            print("\n预警统计：")
            for warning_type, count in warnings.items():
                print(f"{warning_type}: {count}条")
            
            return True
            
        except Exception as e:
            print(f"生成数据时出错: {str(e)}")
            return False

if __name__ == '__main__':
    generator = ProductionDataGenerator()
    generator.generate_all_data()
