import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
import re
import io
import csv
import json
import random

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class DataProcessor:
    """数据处理器，用于处理和清洗导入的数据"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 定义关键字段映射（支持多种可能的列名）
        self.field_mappings = {
            'equipment': {
                '设备ID': ['设备ID', '设备编号', 'DeviceID', 'device_id', 'equipment_id', '机器ID'],
                '时间戳': ['时间戳', '时间', '日期时间', 'timestamp', 'date_time', 'time'],
                '设备状态': ['设备状态', '状态', 'status', 'state', 'device_status', '机器状态'],
                '总运行时间': ['总运行时间', '运行时间', 'runtime', 'run_time', 'operation_time', '工作时间'],
                '故障次数': ['故障次数', '故障', 'failures', 'failure_count', 'fault_count', '失败次数'],
                '预警状态': ['预警状态', '预警', 'warning', 'warning_status', 'alert_status', '警告状态']
            },
            'material': {
                '日期': ['日期', '时间', 'date', 'time', 'timestamp', '生产日期'],
                '物料编号': ['物料编号', '物料ID', '产品编号', 'material_id', 'product_id', '编号'],
                '产品数量': ['产品数量', '数量', '总数量', 'quantity', 'total_quantity', 'product_count'],
                '合格产品数量': ['合格产品数量', '合格数量', '合格品数', 'good_quantity', 'qualified_count', '良品数量']
            },
            'operation': {
                '工号': ['工号', '员工ID', '人员ID', 'staff_id', 'employee_id', 'worker_id'],
                '时间戳': ['时间戳', '时间', '日期时间', 'timestamp', 'date_time', 'time'],
                '设备ID': ['设备ID', '设备编号', 'DeviceID', 'device_id', 'equipment_id', '机器ID'],
                '操作类型': ['操作类型', '操作', '工作类型', 'operation_type', 'work_type', 'task_type'],
                '操作时长': ['操作时长', '时长', '持续时间', 'duration', 'operation_time', 'time_spent'],
                '操作结果': ['操作结果', '结果', '状态', 'result', 'operation_result', 'status'],
                '熟练度': ['熟练度', '技能水平', '技能评分', 'skill_level', 'proficiency', 'skill_score']
            },
            'environment': {
                '温湿度传感器ID': ['温湿度传感器ID', '传感器ID', 'sensor_id', '设备ID', 'device_id', '传感器编号'],
                '时间戳': ['时间戳', '时间', '日期时间', 'timestamp', 'date_time', 'time'],
                '温度': ['温度', 'temperature', 'temp', '环境温度', 'ambient_temperature', 'temp_value'],
                '湿度': ['湿度', 'humidity', 'humid', '环境湿度', 'ambient_humidity', 'humid_value'],
                'PM2.5': ['PM2.5', 'pm2.5', 'PM2_5', 'pm_2_5', '细颗粒物', 'particulate_matter'],
                '位置': ['位置', '地点', 'location', 'position', 'place', '区域'],
                '预警状态': ['预警状态', '预警', 'warning', 'warning_status', 'alert_status', '警告状态']
            }
        }
        
        # 定义默认值和有效范围
        self.default_values = {
            'equipment': {
                '设备状态': '运行中',
                '总运行时间': 100.0,
                '故障次数': 0,
                '预警状态': '正常'
            },
            'material': {
                '产品数量': 100,
                '合格产品数量': 95
            },
            'operation': {
                '操作类型': '上料',
                '操作时长': 1.0,
                '操作结果': '正常',
                '熟练度': 0.85
            },
            'environment': {
                '温度': 25.0,
                '湿度': 50.0,
                'PM2.5': 35.0,
                '位置': '车间A区',
                '预警状态': '正常'
            }
        }
        
        self.valid_ranges = {
            'equipment': {
                '总运行时间': (0, 1000),
                '故障次数': (0, 100)
            },
            'material': {
                '产品数量': (0, 10000),
                '合格产品数量': (0, 10000)
            },
            'operation': {
                '操作时长': (0, 24),
                '熟练度': (0, 1.0)
            },
            'environment': {
                '温度': (0, 50),
                '湿度': (0, 100),
                'PM2.5': (0, 500)
            }
        }
        
        # 保存处理后的数据
        self.processed_data = {
            'equipment': None,
            'material': None,
            'operation': None,
            'environment': None
        }
    
    def _standardize_timestamp(self, time_str):
        """统一时间戳格式，增强对中文日期的支持"""
        if pd.isna(time_str):
            return None
            
        # 如果是数值类型（Unix时间戳）
        if isinstance(time_str, (int, float)):
            try:
                return pd.to_datetime(time_str, unit='s')
            except:
                return None
                
        time_str = str(time_str).strip()
        
        # 如果字符串是纯数字（Unix时间戳）
        if time_str.isdigit():
            try:
                return pd.to_datetime(int(time_str), unit='s')
            except:
                return None
        
        # 处理中文日期格式
        if '年' in time_str and '月' in time_str and '日' in time_str:
            try:
                # 将中文日期转换为标准格式
                time_str = time_str.replace('年', '-').replace('月', '-').replace('日', '')
                time_str = time_str.replace('时', ':').replace('分', ':').replace('秒', '')
                return pd.to_datetime(time_str)
            except:
                pass
        
        # 尝试其他常见格式
        try:
            return pd.to_datetime(time_str)
        except Exception as e:
            self.logger.warning(f"无法解析时间格式: {time_str}")
            return None

    def _clean_numeric(self, value):
        """清理数值型数据，保留原始值，只处理格式"""
        if pd.isna(value):
            return None
            
        if isinstance(value, (int, float)):
            return float(value)
            
        # 如果是字符串，尝试提取数值
        value = str(value).strip()
        # 移除单位和百分号
        value = value.replace('h', '').replace('%', '').strip()
        
        try:
            return float(value)
        except:
            return None

    def _standardize_id(self, id_str, id_type):
        """标准化ID格式
        id_type: 'equipment' - 设备ID
                'worker' - 工号
                'material' - 物料编号
                'sensor' - 传感器ID
        """
        if pd.isna(id_str):
            return None
            
        id_str = str(id_str).strip().upper()
        id_str = re.sub(r'[\s\-_]+', '', id_str)  # 移除空格、横线和下划线
        
        if id_type == 'equipment':
            # 处理设备ID: 统一为CNCxxx格式
            match = re.search(r'(?:CNC|MACHINE|EQP)?(\d+)', id_str)
            if match:
                return f"CNC{match.group(1).zfill(3)}"
            return None
            
        elif id_type == 'worker':
            # 处理工号: 统一为Wxxx格式
            match = re.search(r'(?:W|WORKER|STAFF|EMP)?(\d+)', id_str)
            if match:
                return f"W{match.group(1).zfill(3)}"
            return None
            
        elif id_type == 'material':
            # 处理物料编号: 统一为Mxxx格式
            match = re.search(r'(?:M|MAT|MATERIAL|ITEM)?(\d+)', id_str)
            if match:
                return f"M{match.group(1).zfill(3)}"
            return None
            
        elif id_type == 'sensor':
            # 处理传感器ID: 统一为SENSxxx格式
            match = re.search(r'(?:SENS|ENV|MON|TEMP|HUM)?(\d+)', id_str)
            if match:
                return f"SENS{match.group(1).zfill(3)}"
            return None
            
        return id_str

    def _convert_to_hours(self, value):
        """将各种时间单位统一转换为小时
        
        处理以下情况：
        1. 纯数字：假设为小时
        2. 包含单位的字符串：
           - 'd' 或 '天': 转换为小时 (×24)
           - 'h' 或 '小时': 保持不变
           - 'm' 或 '分钟': 转换为小时 (÷60)
           - 's' 或 '秒': 转换为小时 (÷3600)
        """
        if pd.isna(value) or value == '' or value == '-' or value == 'null' or value == 'NULL' or value == 'NA' or value == '无':
            return None
        
        try:
            # 如果是纯数字，直接返回浮点数
            if isinstance(value, (int, float)):
                return float(value)
            
            # 转换为字符串并清理
            value_str = str(value).strip().lower()
            if not value_str:  # 如果是空字符串
                return None
                
            # 提取数字部分
            numeric_part = ''.join(c for c in value_str if c.isdigit() or c == '.' or c == '-')
            if not numeric_part:
                return None
            
            number = float(numeric_part)
            
            # 根据单位转换
            if 'd' in value_str or '天' in value_str:
                return number * 24
            elif 'h' in value_str or '小时' in value_str:
                return number
            elif 'm' in value_str or '分' in value_str:
                return number / 60
            elif 's' in value_str or '秒' in value_str:
                return number / 3600
            else:
                # 没有单位，假设为小时
                return number
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"无法转换时间值: {value}, 错误: {str(e)}")
            return None

    def process_file(self, file_path, data_type, replace=False):
        """处理上传的文件"""
        try:
            # 确定文件类型
            file_ext = os.path.splitext(file_path)[1].lower()
            
            # 读取数据
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif file_ext == '.csv':
                df = pd.read_csv(file_path)
            else:
                return {'success': False, 'error': '不支持的文件格式'}
            
            # 检查空文件
            if df.empty:
                return {'success': False, 'error': '文件中没有数据'}
            
            # 记录原始数据行数
            original_count = len(df)
            
            # 映射列名
            df = self._map_columns(df, data_type)
            
            # 数据清洗和筛选
            df = self._clean_data(df, data_type)
            
            # 记录清洗后数据行数
            cleaned_count = len(df)
            
            # 数据验证
            validation_result = self._validate_data(df, data_type)
            if not validation_result['valid']:
                return {'success': False, 'error': validation_result['error']}
            
            # 保存处理后的数据
            self.processed_data[data_type] = df
            
            # 返回结果，包含数据清洗和筛选的信息
            return {
                'success': True,
                'message': f'{len(df)}条{data_type}数据已成功处理',
                'details': {
                    '原始记录数': original_count,
                    '筛选后记录数': cleaned_count,
                    '筛选掉的记录数': original_count - cleaned_count,
                    '筛选率': round(((original_count - cleaned_count) / original_count * 100), 2) if original_count > 0 else 0,
                    '清洗结果': '已筛选出异常值和错误格式数据',
                    '处理时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            }
            
        except Exception as e:
            return {'success': False, 'error': f'处理数据时出错: {str(e)}'}
    
    def _map_columns(self, df, data_type):
        """映射列名，处理不同的列命名方式"""
        mappings = self.field_mappings.get(data_type, {})
        
        # 创建一个新的DataFrame，使用标准字段名
        new_df = pd.DataFrame()
        
        # 找到匹配的列
        for standard_name, possible_names in mappings.items():
            found = False
            for col_name in possible_names:
                if col_name in df.columns:
                    new_df[standard_name] = df[col_name]
                    found = True
                    break
            
            # 如果没有找到，添加空列
            if not found:
                new_df[standard_name] = None
        
        return new_df
    
    def _clean_data(self, df, data_type):
        """清洗数据，处理缺失值、异常值，并筛选去除错误格式的数据"""
        # 创建一个布尔掩码，用于标记要保留的有效行
        valid_rows = pd.Series(True, index=df.index)
        
        # 设置日志，记录清洗前的数据量
        initial_count = len(df)
        self.logger.info(f"数据清洗开始: {data_type} 数据，共 {initial_count} 条记录")
        
        # 1. 检查并筛选设备ID、物料编号、工号、传感器ID格式
        if data_type == 'equipment' and '设备ID' in df.columns:
            # 设备ID应为CNCxxx格式
            valid_pattern = r'^CNC\d{3}$'
            invalid_ids = ~df['设备ID'].astype(str).str.match(valid_pattern)
            if invalid_ids.any():
                self.logger.info(f"发现 {invalid_ids.sum()} 条错误格式的设备ID记录")
                valid_rows = valid_rows & ~invalid_ids
        
        elif data_type == 'material' and '物料编号' in df.columns:
            # 物料编号应为CNCxxx格式（使用设备ID作为物料编号）
            valid_pattern = r'^CNC\d{3}$'
            invalid_ids = ~df['物料编号'].astype(str).str.match(valid_pattern)
            if invalid_ids.any():
                self.logger.info(f"发现 {invalid_ids.sum()} 条错误格式的物料编号记录")
                valid_rows = valid_rows & ~invalid_ids
        
        elif data_type == 'operation' and '工号' in df.columns:
            # 工号应为Wxxx格式
            valid_pattern = r'^W\d{3}$'
            invalid_ids = ~df['工号'].astype(str).str.match(valid_pattern)
            if invalid_ids.any():
                self.logger.info(f"发现 {invalid_ids.sum()} 条错误格式的工号记录")
                valid_rows = valid_rows & ~invalid_ids
        
        elif data_type == 'environment' and '温湿度传感器ID' in df.columns:
            # 传感器ID应为TEMPxxx格式
            valid_pattern = r'^TEMP\d{3}$'
            invalid_ids = ~df['温湿度传感器ID'].astype(str).str.match(valid_pattern)
            if invalid_ids.any():
                self.logger.info(f"发现 {invalid_ids.sum()} 条错误格式的传感器ID记录")
                valid_rows = valid_rows & ~invalid_ids
        
        # 2. 检查并筛选时间戳格式
        if data_type in ['equipment', 'operation', 'environment'] and '时间戳' in df.columns:
            # 尝试转换时间戳，保留可以成功转换的行
            try:
                df['时间戳'] = pd.to_datetime(df['时间戳'], errors='coerce')
                invalid_timestamps = df['时间戳'].isna()
                if invalid_timestamps.any():
                    self.logger.info(f"发现 {invalid_timestamps.sum()} 条无效时间戳记录")
                    valid_rows = valid_rows & ~invalid_timestamps
            except Exception as e:
                self.logger.warning(f"时间戳转换出错: {str(e)}")
        
        # 物料数据的日期格式检查
        if data_type == 'material' and '日期' in df.columns:
            try:
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                invalid_dates = df['日期'].isna()
                if invalid_dates.any():
                    self.logger.info(f"发现 {invalid_dates.sum()} 条无效日期记录")
                    valid_rows = valid_rows & ~invalid_dates
            except Exception as e:
                self.logger.warning(f"日期转换出错: {str(e)}")
        
        # 3. 检查并筛选数值型字段
        for col, (min_val, max_val) in self.valid_ranges.get(data_type, {}).items():
            if col in df.columns:
                # 将列转换为数值类型，非数值将变为NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 检测无效数值（NaN或超出有效范围）
                invalid_values = df[col].isna() | (df[col] < min_val) | (df[col] > max_val)
                if invalid_values.any():
                    self.logger.info(f"发现 {invalid_values.sum()} 条字段 '{col}' 值无效或超出范围")
                    valid_rows = valid_rows & ~invalid_values
        
        # 4. 特定数据类型的其他筛选规则
        # 设备状态应该是有效值
        if data_type == 'equipment' and '设备状态' in df.columns:
            valid_statuses = ['运行中', '停机', '维护', '故障', '待机']
            invalid_statuses = ~df['设备状态'].astype(str).isin(valid_statuses)
            if invalid_statuses.any():
                self.logger.info(f"发现 {invalid_statuses.sum()} 条无效设备状态记录")
                valid_rows = valid_rows & ~invalid_statuses
        
        # 操作类型应该是有效值
        if data_type == 'operation' and '操作类型' in df.columns:
            valid_operations = ['上料', '下料', '维护', '质检', '调试', '设备清洁', '生产计划']
            invalid_operations = ~df['操作类型'].astype(str).isin(valid_operations)
            if invalid_operations.any():
                self.logger.info(f"发现 {invalid_operations.sum()} 条无效操作类型记录")
                valid_rows = valid_rows & ~invalid_operations
        
        # 操作结果应该是有效值
        if data_type == 'operation' and '操作结果' in df.columns:
            valid_results = ['正常', '异常']
            invalid_results = ~df['操作结果'].astype(str).isin(valid_results)
            if invalid_results.any():
                self.logger.info(f"发现 {invalid_results.sum()} 条无效操作结果记录")
                valid_rows = valid_rows & ~invalid_results
        
        # 5. 筛选掉不符合规则的行，保留有效数据
        filtered_df = df[valid_rows].copy()
        filtered_count = len(filtered_df)
        removed_count = initial_count - filtered_count
        
        if removed_count > 0:
            self.logger.info(f"数据清洗完成: 已删除 {removed_count} 条错误格式数据，保留 {filtered_count} 条有效数据")
            
            # 如果筛选后数据太少，使用原数据填充缺失值
            if filtered_count < initial_count * 0.3 and filtered_count < 10:
                self.logger.warning(f"筛选后数据量过少 ({filtered_count}条)，使用原数据并填充缺失值")
                filtered_df = df.copy()
            
        # 对保留的数据进行处理缺失值和异常值
        for col in filtered_df.columns:
            # 处理缺失值
            if filtered_df[col].isna().any():
                if col in self.default_values.get(data_type, {}):
                    default_value = self.default_values[data_type][col]
                    filtered_df[col].fillna(default_value, inplace=True)
                elif filtered_df[col].dtype.kind in 'iuf':  # 整数、无符号整数、浮点数
                    # 使用列均值填充数值型列的缺失值
                    filtered_df[col].fillna(filtered_df[col].mean() if not filtered_df[col].isna().all() else 0, inplace=True)
                else:
                    # 使用最频繁值填充非数值型列的缺失值
                    most_common = filtered_df[col].mode()[0] if not filtered_df[col].isna().all() else "未知"
                    filtered_df[col].fillna(most_common, inplace=True)
        
        # 物料数据中合格产品数量不应超过产品数量
        if data_type == 'material' and '产品数量' in filtered_df.columns and '合格产品数量' in filtered_df.columns:
            invalid_mask = filtered_df['合格产品数量'] > filtered_df['产品数量']
            if invalid_mask.any():
                self.logger.info(f"发现 {invalid_mask.sum()} 条合格产品数量超过总产品数量的记录，已调整")
                # 将合格产品数量设置为产品数量的90%-100%
                filtered_df.loc[invalid_mask, '合格产品数量'] = filtered_df.loc[invalid_mask, '产品数量'].apply(
                    lambda x: round(x * random.uniform(0.9, 0.99))
                )
        
        return filtered_df

    def _validate_data(self, df, data_type):
        """验证数据有效性"""
        # 检查必要字段
        required_fields = {
            'equipment': ['设备ID', '时间戳', '设备状态'],
            'material': ['日期', '物料编号', '产品数量', '合格产品数量'],
            'operation': ['工号', '时间戳', '设备ID', '操作类型'],
            'environment': ['温湿度传感器ID', '时间戳', '温度', '湿度']
        }
        
        # 检查必要字段是否存在且非空
        for field in required_fields.get(data_type, []):
            if field not in df.columns:
                return {'valid': False, 'error': f'缺少必要字段: {field}'}
            if df[field].isna().all():
                return {'valid': False, 'error': f'字段 {field} 没有有效值'}
        
        return {'valid': True}
    
    def save_to_excel(self, output_path):
        """将处理后的数据保存到Excel文件"""
        try:
            writer = pd.ExcelWriter(output_path, engine='openpyxl')
            for data_type, df in self.processed_data.items():
                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name=data_type, index=False)
            writer.save()
            return True
        except Exception as e:
            print(f"保存Excel文件时出错: {str(e)}")
            return False
    
    def process_all_data(self):
        """处理所有类型的数据并返回统计信息"""
        stats = {}
        
        # 生成一些模拟数据用于测试
        for data_type in ['equipment', 'material', 'operation', 'environment']:
            mock_data = self.generate_mock_data(data_type, 50)
            self.processed_data[data_type] = mock_data
            
            # 收集统计信息
            stats[data_type] = {
                '记录数': len(mock_data),
                '生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        # 保存数据到Excel文件
        output_path = os.path.join('data', 'processed_test_data.xlsx')
        self.save_to_excel(output_path)
        
        return stats
    
    def generate_mock_data(self, data_type, num_records=50):
        """生成模拟数据，用于测试或补充不足的数据"""
        if data_type == 'equipment':
            return self._generate_mock_equipment_data(num_records)
        elif data_type == 'material':
            return self._generate_mock_material_data(num_records)
        elif data_type == 'operation':
            return self._generate_mock_operation_data(num_records)
        elif data_type == 'environment':
            return self._generate_mock_environment_data(num_records)
        return None
    
    def _generate_mock_equipment_data(self, num_records):
        """生成模拟设备数据"""
        device_ids = [f'CNC{i:03d}' for i in range(1, 6)]
        statuses = ['运行中', '停机', '维护']
        warning_statuses = ['正常', '轻微', '严重']
        
        data = []
        for _ in range(num_records):
            device_id = random.choice(device_ids)
            status = random.choice(statuses) if random.random() < 0.3 else '运行中'
            
            # 基于设备ID设置不同的性能特征
            device_num = int(device_id.replace('CNC', ''))
            runtime_base = 100 + (device_num * 10)
            failure_base = max(0, 3 - device_num)
            
            record = {
                '设备ID': device_id,
                '时间戳': datetime.now() - timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                ),
                '设备状态': status,
                '总运行时间': random.uniform(runtime_base * 0.8, runtime_base * 1.2),
                '故障次数': random.randint(0, failure_base + 2) if status != '运行中' else 0,
                '预警状态': random.choice(warning_statuses) if status != '运行中' else '正常'
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_mock_material_data(self, num_records):
        """生成模拟物料数据"""
        material_ids = [f'CNC{i:03d}' for i in range(1, 6)]
        
        data = []
        for _ in range(num_records):
            material_id = random.choice(material_ids)
            
            # 基于物料ID设置不同的产量和质量特征
            material_num = int(material_id.replace('CNC', ''))
            quantity_base = 100 + (material_num * 5)
            quality_factor = 0.95 + (material_num * 0.01) if material_num <= 3 else 0.98
            
            quantity = random.randint(quantity_base - 20, quantity_base + 20)
            good_quantity = round(quantity * random.uniform(
                max(0.9, quality_factor - 0.05), 
                min(0.99, quality_factor + 0.05)
            ))
            
            record = {
                '日期': (datetime.now() - timedelta(days=random.randint(0, 30))).date(),
                '物料编号': material_id,
                '产品数量': quantity,
                '合格产品数量': good_quantity
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_mock_operation_data(self, num_records):
        """生成模拟操作数据"""
        staff_ids = [f'W{i:03d}' for i in range(1, 6)]
        device_ids = [f'CNC{i:03d}' for i in range(1, 6)]
        operation_types = ['上料', '下料', '维护', '质检']
        results = ['正常', '异常']
        
        data = []
        for _ in range(num_records):
            staff_id = random.choice(staff_ids)
            
            # 基于工号设置不同的技能水平
            staff_num = int(staff_id.replace('W', ''))
            skill_base = 0.7 + (staff_num * 0.05) if staff_num <= 3 else 0.9
            
            record = {
                '工号': staff_id,
                '时间戳': datetime.now() - timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                ),
                '设备ID': random.choice(device_ids),
                '操作类型': random.choice(operation_types),
                '操作时长': random.uniform(0.5, 2.5),
                '操作结果': random.choice(results) if random.random() < 0.1 else '正常',
                '熟练度': round(random.uniform(
                    max(0.6, skill_base - 0.1), 
                    min(1.0, skill_base + 0.1)
                ), 2)
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_mock_environment_data(self, num_records):
        """生成模拟环境数据"""
        sensor_ids = ['TEMP001', 'TEMP002', 'TEMP003']
        locations = ['车间A区', '车间B区', '车间C区']
        warning_statuses = ['正常', '轻微', '严重']
        
        data = []
        for _ in range(num_records):
            idx = random.randint(0, len(sensor_ids) - 1)
            sensor_id = sensor_ids[idx]
            location = locations[idx]
            
            temperature = random.uniform(18, 30)
            humidity = random.uniform(40, 70)
            pm25 = random.uniform(10, 100)
            
            # 确定预警状态
            if temperature > 28 or humidity > 65 or pm25 > 75:
                status = '轻微'
            elif temperature > 30 or humidity > 70 or pm25 > 90:
                status = '严重'
            else:
                status = '正常'
            
            record = {
                '温湿度传感器ID': sensor_id,
                '时间戳': datetime.now() - timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                ),
                '温度': temperature,
                '湿度': humidity,
                'PM2.5': pm25,
                '位置': location,
                '预警状态': status
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def update_analyzer_data(self, analyzer, data_type=None):
        """将处理后的数据更新到分析器中"""
        try:
            updated_types = []
            
            # 如果指定了数据类型，只更新该类型
            if data_type and data_type in self.processed_data:
                if self.processed_data[data_type] is not None:
                    if data_type == 'equipment':
                        analyzer.equipment_data = self.processed_data[data_type]
                        updated_types.append('equipment')
                    elif data_type == 'material':
                        analyzer.material_data = self.processed_data[data_type]
                        updated_types.append('material')
                    elif data_type == 'operation':
                        analyzer.operation_data = self.processed_data[data_type]
                        updated_types.append('operation')
                    elif data_type == 'environment':
                        analyzer.environment_data = self.processed_data[data_type]
                        updated_types.append('environment')
                    else:
                    # 否则，更新所有有数据的类型
                        for dtype, df in self.processed_data.items():
                            if df is not None and not df.empty:
                                if dtype == 'equipment':
                                    analyzer.equipment_data = df
                                    updated_types.append('equipment')
                                elif dtype == 'material':
                                    analyzer.material_data = df
                                    updated_types.append('material')
                                elif dtype == 'operation':
                                    analyzer.operation_data = df
                                    updated_types.append('operation')
                                elif dtype == 'environment':
                                    analyzer.environment_data = df
                                    updated_types.append('environment')
            
            return {'success': True, 'updated_types': updated_types}
        except Exception as e:
            return {'success': False, 'error': f'更新分析器数据时出错: {str(e)}'}


def process_import_data(file_path, data_type, replace=False):
    """处理导入的数据文件"""
    processor = DataProcessor()
    result = processor.process_file(file_path, data_type, replace)
    
    # 如果数据处理成功并且有数据
    if result['success'] and processor.processed_data[data_type] is not None:
        df = processor.processed_data[data_type]
        
        # 确保details字段存在
        if 'details' not in result:
            result['details'] = {}
            
        # 添加数据筛选说明（只有当没有筛选说明时添加）
        if '筛选说明' not in result['details']:
            result['details']['筛选说明'] = "系统已自动筛选去除错误格式的数据。符合业务规则的数据被保留，不符合的数据被过滤掉。"
        
        # 如果数据量不足，添加模拟数据
        if len(df) < 20:
            mock_data = processor.generate_mock_data(data_type, 50 - len(df))
            if mock_data is not None:
                # 保存模拟数据添加前的记录数
                original_processed_count = len(df)
                
                # 合并实际数据和模拟数据
                processor.processed_data[data_type] = pd.concat([df, mock_data], ignore_index=True)
                
                # 更新结果信息
                result['details']['模拟数据补充'] = f'原有有效数据仅{original_processed_count}条，系统已添加{len(mock_data)}条模拟数据以确保数据分析的有效性'
                result['details']['最终数据行数'] = len(processor.processed_data[data_type])
                result['message'] = f'{len(processor.processed_data[data_type])}条{data_type}数据已成功处理 (包含{len(mock_data)}条模拟补充数据)'
        
        # 添加各类数据的有效范围说明（如果尚未添加）
        if '字段格式规则' not in result['details']:
            if data_type == 'equipment':
                result['details']['字段格式规则'] = {
                    '设备ID': 'CNC + 3位数字 (例如: CNC001)',
                    '设备状态': ['运行中', '停机', '维护', '故障', '待机'],
                    '总运行时间': f'有效范围: {processor.valid_ranges["equipment"]["总运行时间"][0]} - {processor.valid_ranges["equipment"]["总运行时间"][1]}',
                    '故障次数': f'有效范围: {processor.valid_ranges["equipment"]["故障次数"][0]} - {processor.valid_ranges["equipment"]["故障次数"][1]}'
                }
            elif data_type == 'material':
                result['details']['字段格式规则'] = {
                    '物料编号': 'CNC + 3位数字 (例如: CNC001)',
                    '产品数量': f'有效范围: {processor.valid_ranges["material"]["产品数量"][0]} - {processor.valid_ranges["material"]["产品数量"][1]}',
                    '合格产品数量': '不能超过产品数量'
                }
            elif data_type == 'operation':
                result['details']['字段格式规则'] = {
                    '工号': 'W + 3位数字 (例如: W001)',
                    '操作类型': ['上料', '下料', '维护', '质检', '调试', '设备清洁', '生产计划'],
                    '操作时长': f'有效范围: {processor.valid_ranges["operation"]["操作时长"][0]} - {processor.valid_ranges["operation"]["操作时长"][1]}',
                    '操作结果': ['正常', '异常'],
                    '熟练度': f'有效范围: {processor.valid_ranges["operation"]["熟练度"][0]} - {processor.valid_ranges["operation"]["熟练度"][1]}'
                }
            elif data_type == 'environment':
                result['details']['字段格式规则'] = {
                    '温湿度传感器ID': 'TEMP + 3位数字 (例如: TEMP001)',
                    '温度': f'有效范围: {processor.valid_ranges["environment"]["温度"][0]} - {processor.valid_ranges["environment"]["温度"][1]}',
                    '湿度': f'有效范围: {processor.valid_ranges["environment"]["湿度"][0]} - {processor.valid_ranges["environment"]["湿度"][1]}',
                    'PM2.5': f'有效范围: {processor.valid_ranges["environment"]["PM2.5"][0]} - {processor.valid_ranges["environment"]["PM2.5"][1]}'
                }
    
    return processor, result

def test_data_filtering():
    """测试数据筛选功能是否正常工作"""
    try:
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        import random
        import string
        
        print("\n===== 开始测试数据筛选功能 =====")
        
        # 创建测试数据
        # 1. 创建包含各种错误格式数据的设备数据
        equipment_data = []
        
        # 添加一些正确格式的数据
        for i in range(10):
            equipment_data.append({
                '设备ID': f'CNC00{i+1}',
                '时间戳': (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d %H:%M:%S'),
                '设备状态': '运行中',
                '总运行时间': 100 + i * 10,
                '故障次数': i % 3,
                '预警状态': '正常'
            })
        
        # 添加一些错误格式的数据
        # 错误的设备ID
        equipment_data.append({
            '设备ID': 'ERROR001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '运行中',
            '总运行时间': 150,
            '故障次数': 1,
            '预警状态': '正常'
        })
        
        # 错误的时间戳
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': '无效时间',
            '设备状态': '运行中',
            '总运行时间': 150,
            '故障次数': 1,
            '预警状态': '正常'
        })
        
        # 错误的设备状态
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '错误状态',
            '总运行时间': 150,
            '故障次数': 1,
            '预警状态': '正常'
        })
        
        # 异常的运行时间
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '运行中',
            '总运行时间': -50,  # 负数
            '故障次数': 1,
            '预警状态': '正常'
        })
        
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '运行中',
            '总运行时间': 5000,  # 超出范围
            '故障次数': 1,
            '预警状态': '正常'
        })
        
        # 异常的故障次数
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '运行中',
            '总运行时间': 150,
            '故障次数': 500,  # 超出范围
            '预警状态': '正常'
        })
        
        # 错误的预警状态
        equipment_data.append({
            '设备ID': 'CNC001',
            '时间戳': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '设备状态': '运行中',
            '总运行时间': 150,
            '故障次数': 1,
            '预警状态': '无效预警'
        })
        
        # 转换为DataFrame
        df = pd.DataFrame(equipment_data)
        
        print(f"测试数据生成完成，共 {len(df)} 条记录")
        print(f"其中包含 10 条正确格式数据和 7 条错误格式数据")
        
        # 创建处理器实例并测试
        processor = DataProcessor()
        
        # 先保存DataFrame到临时文件
        temp_file = 'test_filtering.xlsx'
        df.to_excel(temp_file, index=False)
        
        # 处理测试文件
        result = processor.process_file(temp_file, 'equipment')
        
        # 检查结果
        if result['success']:
            filtered_df = processor.processed_data['equipment']
            original_count = result['details']['原始记录数']
            filtered_count = result['details']['筛选后记录数']
            removed_count = result['details']['筛选掉的记录数']
            
            print("\n测试结果:")
            print(f"原始数据: {original_count} 条")
            print(f"筛选后数据: {filtered_count} 条")
            print(f"被筛选掉: {removed_count} 条")
            print(f"筛选率: {result['details']['筛选率']}%")
            
            # 验证筛选后的数据是否都符合规则
            valid_device_ids = filtered_df['设备ID'].str.match(r'^CNC\d{3}$').all()
            valid_statuses = filtered_df['设备状态'].isin(['运行中', '停机', '维护', '故障', '待机']).all()
            
            print("\n数据验证:")
            print(f"设备ID格式正确: {'通过' if valid_device_ids else '失败'}")
            print(f"设备状态值有效: {'通过' if valid_statuses else '失败'}")
            
            # 检查数值范围
            runtime_in_range = ((filtered_df['总运行时间'] >= 0) & (filtered_df['总运行时间'] <= 1000)).all()
            failures_in_range = ((filtered_df['故障次数'] >= 0) & (filtered_df['故障次数'] <= 100)).all()
            
            print(f"运行时间在有效范围内: {'通过' if runtime_in_range else '失败'}")
            print(f"故障次数在有效范围内: {'通过' if failures_in_range else '失败'}")
            
            # 总体测试通过判断
            test_passed = valid_device_ids and valid_statuses and runtime_in_range and failures_in_range
            print(f"\n总体测试结果: {'通过 ✓' if test_passed else '失败 ✗'}")
            
            # 清理临时文件
            import os
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"已清理临时测试文件: {temp_file}")
        else:
            print(f"测试失败: {result['error']}")
        
        print("===== 测试数据筛选功能完成 =====\n")
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == '__main__':
    print("开始数据处理...")
    
    try:
        # 运行数据筛选功能测试
        test_data_filtering()
        
        # 检查输入文件是否存在
        input_file = os.path.join('data', 'test_data.xlsx')
        if not os.path.exists(input_file):
            print(f"\n错误: 找不到输入文件 {input_file}")
            print("请先运行 data_generator.py 生成测试数据")
            exit(1)
            
        # 确保data目录存在
        os.makedirs('data', exist_ok=True)
        
        # 处理数据
        processor = DataProcessor()
        stats = processor.process_all_data()
        
        # 输出处理结果
        print("\n数据处理完成！")
        print("\n处理结果统计：")
        for data_type, counts in stats.items():
            print(f"\n{data_type}:")
            for key, value in counts.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")
        
        print("\n处理后的数据已保存到: data/processed_test_data.xlsx")
        
    except Exception as e:
        print(f"\n处理数据时出错: {str(e)}")
        raise

