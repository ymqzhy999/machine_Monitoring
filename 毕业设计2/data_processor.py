import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import re

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
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
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

    def process_equipment_data(self, df):
        """处理设备数据"""
        df = df.copy()
        
        # 统一时间戳格式
        df['时间戳'] = df['时间戳'].apply(self._standardize_timestamp)
        
        # 标准化设备ID
        df['设备ID'] = df['设备ID'].apply(lambda x: self._standardize_id(x, 'equipment'))
        
        # 转换时间相关字段为小时单位
        time_columns = ['总运行时间', '故障持续时间']
        for col in time_columns:
            if col in df.columns:
                # 先处理特殊的空值
                df[col] = df[col].replace(['null', 'NULL', 'NA', '无', '-', ''], np.nan)
                # 然后转换时间
                df[col] = df[col].apply(self._convert_to_hours)
        
        # 处理数值型字段
        numeric_columns = ['振动值', '温度']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_numeric)
        
        # 统一其他列的空值表示
        df = df.replace(['null', 'NULL', 'NA', '无', '-', ''], np.nan)
        
        return df

    def process_operation_data(self, df):
        """处理操作数据"""
        df = df.copy()
        
        # 统一时间戳格式
        df['时间戳'] = df['时间戳'].apply(self._standardize_timestamp)
        
        # 标准化工号
        df['工号'] = df['工号'].apply(lambda x: self._standardize_id(x, 'worker'))
        
        # 转换操作时长为小时单位
        if '操作时长' in df.columns:
            df['操作时长'] = df['操作时长'].apply(self._convert_to_hours)
        
        # 处理数值型字段
        if '熟练度' in df.columns:
            df['熟练度'] = df['熟练度'].apply(self._clean_numeric)
        
        # 统一空值表示
        df = df.replace(['null', 'NULL', 'NA', '未知', '-', ''], np.nan)
        
        return df

    def process_material_data(self, df):
        """处理物料数据"""
        df = df.copy()
        
        # 统一时间戳格式
        if '日期' in df.columns:
            df['日期'] = df['日期'].apply(self._standardize_timestamp)
        
        # 标准化物料编号
        df['物料编号'] = df['物料编号'].apply(lambda x: self._standardize_id(x, 'material'))
        
        # 处理数值型字段
        numeric_columns = ['物料投入量', '物料使用量', '产品数量', '合格产品数量']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_numeric)
        
        # 统一空值表示
        df = df.replace(['null', 'NULL', 'NA', '未记录', '-', ''], np.nan)
        
        return df

    def process_environment_data(self, df):
        """处理环境数据"""
        df = df.copy()
        
        # 统一时间戳格式
        df['时间戳'] = df['时间戳'].apply(self._standardize_timestamp)
        if '采集时间' in df.columns:
            df['采集时间'] = df['采集时间'].apply(self._standardize_timestamp)
        
        # 标准化传感器ID
        if '温湿度传感器ID' in df.columns:
            df['温湿度传感器ID'] = df['温湿度传感器ID'].apply(lambda x: self._standardize_id(x, 'sensor'))
        
        # 处理数值型字段
        numeric_columns = ['温度', '湿度', 'PM2.5']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_numeric)
        
        # 删除采集状态列
        if '采集状态' in df.columns:
            df = df.drop('采集状态', axis=1)
        
        # 统一空值表示
        df = df.replace(['null', 'NULL', 'NA', '未知', '-', ''], np.nan)
        
        return df
    
    def process_all_data(self):
        """处理所有数据"""
        self.logger.info("开始处理数据...")
        
        try:
            # 读取Excel文件
            input_file = os.path.join('data', 'test_data.xlsx')
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"找不到输入文件: {input_file}")
            
            # 读取各个sheet的数据
            equipment_data = pd.read_excel(input_file, sheet_name='设备数据')
            operation_data = pd.read_excel(input_file, sheet_name='人员操作数据')
            material_data = pd.read_excel(input_file, sheet_name='物料数据')
            environment_data = pd.read_excel(input_file, sheet_name='环境数据')
            
            self.logger.info("数据读取完成，开始处理...")
            
            # 处理各类数据
            processed_equipment = self.process_equipment_data(equipment_data)
            processed_operation = self.process_operation_data(operation_data)
            processed_material = self.process_material_data(material_data)
            processed_environment = self.process_environment_data(environment_data)
            
            # 验证时间单位转换结果
            self.logger.info("\n=== 时间单位转换结果验证 ===")
            self.logger.info("设备数据:")
            if '总运行时间' in processed_equipment.columns:
                self.logger.info(f"总运行时间范围: {processed_equipment['总运行时间'].min():.2f} - {processed_equipment['总运行时间'].max():.2f} 小时")
            if '故障持续时间' in processed_equipment.columns:
                self.logger.info(f"故障持续时间范围: {processed_equipment['故障持续时间'].min():.2f} - {processed_equipment['故障持续时间'].max():.2f} 小时")
            
            self.logger.info("\n人员操作数据:")
            if '操作时长' in processed_operation.columns:
                self.logger.info(f"操作时长范围: {processed_operation['操作时长'].min():.2f} - {processed_operation['操作时长'].max():.2f} 小时")
            
            # 保存处理后的数据
            output_file = os.path.join('data', 'processed_test_data.xlsx')
            with pd.ExcelWriter(output_file) as writer:
                processed_equipment.to_excel(writer, sheet_name='设备数据', index=False)
                processed_operation.to_excel(writer, sheet_name='人员操作数据', index=False)
                processed_material.to_excel(writer, sheet_name='物料数据', index=False)
                processed_environment.to_excel(writer, sheet_name='环境数据', index=False)
            
            self.logger.info(f"\n数据处理完成，已保存到: {output_file}")
            
            # 返回处理结果统计
            stats = {
                '设备数据': {
                    '记录数': len(processed_equipment),
                    '有效设备ID数': processed_equipment['设备ID'].notna().sum(),
                    '时间戳完整率': (1 - processed_equipment['时间戳'].isna().sum() / len(processed_equipment)) * 100
                },
                '人员操作数据': {
                    '记录数': len(processed_operation),
                    '有效工号数': processed_operation['工号'].notna().sum(),
                    '时间戳完整率': (1 - processed_operation['时间戳'].isna().sum() / len(processed_operation)) * 100
                },
                '物料数据': {
                    '记录数': len(processed_material),
                    '有效物料编号数': processed_material['物料编号'].notna().sum(),
                    '日期完整率': (1 - processed_material['日期'].isna().sum() / len(processed_material)) * 100
                },
                '环境数据': {
                    '记录数': len(processed_environment),
                    '有效传感器ID数': processed_environment['温湿度传感器ID'].notna().sum() if '温湿度传感器ID' in processed_environment.columns else 0,
                    '时间戳完整率': (1 - processed_environment['时间戳'].isna().sum() / len(processed_environment)) * 100
                }
            }
            
            # 打印统计结果
            self.logger.info("\n=== 处理结果统计 ===")
            for data_type, metrics in stats.items():
                self.logger.info(f"\n{data_type}:")
                for metric_name, value in metrics.items():
                    if isinstance(value, float):
                        self.logger.info(f"  {metric_name}: {value:.2f}")
                    else:
                        self.logger.info(f"  {metric_name}: {value}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"数据处理过程中发生错误: {str(e)}")
            raise

if __name__ == '__main__':
    print("开始数据处理...")
    
    try:
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
                print(f"  {key}: {value}")
        
        print("\n处理后的数据已保存到: data/processed_test_data.xlsx")
        
    except Exception as e:
        print(f"\n处理数据时出错: {str(e)}")
        raise

