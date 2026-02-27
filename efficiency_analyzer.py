import pandas as pd
import numpy as np
import os
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('efficiency_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EfficiencyAnalyzer:
    def __init__(self, equipment_data, operation_data, material_data, environment_data):
        self.logger = logging.getLogger(__name__)
        self.equipment_df = equipment_data
        self.operation_df = operation_data
        self.material_df = material_data
        self.environment_df = environment_data
        
        # 数据验证
        self._validate_data()
    
    def _validate_data(self):
        """验证数据完整性和合理性"""
        # 检查设备数据
        self.logger.info("\n=== 设备数据检查 ===")
        self.logger.info(f"设备总记录数: {len(self.equipment_df)}")
        self.logger.info(f"唯一设备数: {self.equipment_df['设备ID'].nunique()}")
        self.logger.info(f"设备状态分布:\n{self.equipment_df['状态'].value_counts()}")
        self.logger.info(f"总运行时间范围: {self.equipment_df['总运行时间'].min()} - {self.equipment_df['总运行时间'].max()}")
        self.logger.info(f"故障持续时间范围: {self.equipment_df['故障持续时间'].min()} - {self.equipment_df['故障持续时间'].max()}")
        
        # 检查物料数据
        self.logger.info("\n=== 物料数据检查 ===")
        self.logger.info(f"物料记录总数: {len(self.material_df)}")
        self.logger.info(f"产品数量总和: {self.material_df['产品数量'].sum()}")
        self.logger.info(f"合格产品数量总和: {self.material_df['合格产品数量'].sum()}")
        self.logger.info(f"批次数量: {self.material_df['批次号'].nunique()}")
        
        # 检查人员操作数据
        self.logger.info("\n=== 人员操作数据检查 ===")
        self.logger.info(f"操作记录总数: {len(self.operation_df)}")
        self.logger.info(f"操作时长范围: {self.operation_df['操作时长'].min()} - {self.operation_df['操作时长'].max()}")
        self.logger.info(f"质检结果分布:\n{self.operation_df['质检结果'].value_counts()}")

    def calculate_oee(self):
        """计算设备综合效率(OEE)
        OEE = 可用性 × 性能效率 × 质量率
        """
        self.logger.info("\n=== OEE计算详情 ===")
        
        # 1. 计算可用性 = (计划生产时间 - 停机时间) / 计划生产时间
        running_states = ['运行', '运行中', '待机']
        fault_states = ['故障', '故障中', '停机']
        
        # 按设备分组计算运行时间和停机时间
        device_times = {}
        total_actual_output = 0
        total_theoretical_output = 0
        
        for device in self.equipment_df['设备ID'].unique():
            device_data = self.equipment_df[self.equipment_df['设备ID'] == device]
            if len(device_data) > 0:
                # 获取最新一条记录的总运行时间作为累计运行时间
                latest_record = device_data.sort_values('时间戳', ascending=False).iloc[0]
                planned_time = latest_record['总运行时间'] if pd.notna(latest_record['总运行时间']) else 0
                
                # 计算该设备的停机时间（当天的故障时间）
                current_date = latest_record['时间戳'].date()
                today_faults = device_data[
                    (device_data['状态'].isin(fault_states)) & 
                    (device_data['时间戳'].dt.date == current_date)
                ]
                downtime = today_faults['故障持续时间'].fillna(0).sum()
                
                device_times[device] = {
                    'planned_time': planned_time,
                    'downtime': downtime
                }
        
        # 计算总的可用性
        total_planned_time = sum(d['planned_time'] for d in device_times.values())
        total_downtime = sum(d['downtime'] for d in device_times.values())
        
        # 确保计划时间大于0且停机时间不超过计划时间
        if total_planned_time > 0:
            total_downtime = min(total_downtime, total_planned_time)
            availability = (total_planned_time - total_downtime) / total_planned_time
        else:
            availability = 0
        
        self.logger.info(f"\n总计划时间: {total_planned_time:.2f}小时")
        self.logger.info(f"总运行时间: {total_planned_time - total_downtime:.2f}小时")
        self.logger.info(f"总停机时间: {total_downtime:.2f}小时")
        self.logger.info(f"可用性: {availability:.2%}")
        
        # 2. 计算性能效率 = 实际产出 / 理论产出
        # 获取最近一天的数据
        latest_date = self.material_df['日期'].max()
        if pd.notna(latest_date):
            today_data = self.material_df[
                pd.to_datetime(self.material_df['日期']).dt.date == pd.to_datetime(latest_date).date()
            ]
            
            # 实际产出（当天）
            actual_output = today_data['产品数量'].fillna(0).sum()
            
            # 理论产出 = 实际运行时间 × 标准产能
            actual_running_time = total_planned_time - total_downtime
            
            # 使用历史最高产能作为标准产能
            max_hourly_output = (
                self.material_df.groupby(pd.to_datetime(self.material_df['日期']).dt.date)['产品数量']
                .sum()
                .max() / 24  # 假设24小时运行
            )
            
            theoretical_output = actual_running_time * max_hourly_output if max_hourly_output > 0 else actual_output
        else:
            actual_output = 0
            theoretical_output = 0
        
        performance = actual_output / theoretical_output if theoretical_output > 0 else 0
        
        # 确保性能效率不超过1且不为0
        performance = min(max(performance, 0.1), 1.0)  # 设置最小值为0.1
        
        self.logger.info(f"\n实际产出: {actual_output:.2f}")
        self.logger.info(f"理论产出: {theoretical_output:.2f}")
        self.logger.info(f"性能效率: {performance:.2%}")
        
        # 3. 计算质量率 = 合格品数量 / 总产品数量
        if pd.notna(latest_date):
            today_data = self.material_df[
                pd.to_datetime(self.material_df['日期']).dt.date == pd.to_datetime(latest_date).date()
            ]
            total_products = today_data['产品数量'].fillna(0).sum()
            good_products = today_data['合格产品数量'].fillna(0).sum()
        else:
            total_products = 0
            good_products = 0
        
        # 确保合格品数量不超过总产品数量
        good_products = min(good_products, total_products)
        quality = good_products / total_products if total_products > 0 else 0
        
        # 确保质量率不为0
        quality = max(quality, 0.1)  # 设置最小值为0.1
        
        self.logger.info(f"\n总产品数: {total_products:.2f}")
        self.logger.info(f"合格品数: {good_products:.2f}")
        self.logger.info(f"质量率: {quality:.2%}")
        
        # 计算OEE并确保不超过100%
        oee = min(availability * performance * quality, 1.0)
        
        self.logger.info(f"\nOEE = {availability:.2%} × {performance:.2%} × {quality:.2%} = {oee:.2%}")
        
        return {
            'OEE': oee,
            '可用性': availability,
            '性能效率': performance,
            '质量率': quality,
            '总计划时间': total_planned_time
        }
    
    def calculate_teep(self):
        """计算整体设备效率(TEEP)
        TEEP = OEE × 设备利用率
        设备利用率 = 计划生产时间 / 日历时间
        """
        self.logger.info("\n=== TEEP计算详情 ===")
        
        oee_metrics = self.calculate_oee()
        oee = oee_metrics['OEE']
        
        # 计算设备利用率
        # 日历时间固定为24小时
        calendar_time = 24.0
        
        # 使用OEE计算中的总计划时间
        planned_production_time = oee_metrics['总计划时间'] if '总计划时间' in oee_metrics else 0
        
        # 确保计划生产时间不超过日历时间
        planned_production_time = min(planned_production_time, calendar_time)
        
        self.logger.info(f"日历时间: {calendar_time:.2f}小时")
        self.logger.info(f"计划生产时间: {planned_production_time:.2f}小时")
        
        # 设备利用率
        utilization = planned_production_time / calendar_time if calendar_time > 0 else 0
        
        # 确保利用率不超过100%
        utilization = min(utilization, 1.0)
        
        self.logger.info(f"设备利用率: {utilization:.2%}")
        
        # 计算TEEP并确保不超过100%
        teep = min(oee * utilization, 1.0)
        self.logger.info(f"TEEP = {oee:.2%} × {utilization:.2%} = {teep:.2%}")
        
        return teep
    
    def calculate_capacity_utilization(self):
        """计算产能利用率"""
        actual_output = self.material_df['产品数量'].fillna(0).sum()
        max_capacity = self.material_df['物料投入量'].fillna(0).sum()
        
        return actual_output / max_capacity if max_capacity > 0 else 0
    
    def calculate_cycle_time(self):
        """计算生产周期时间（小时）"""
        # 按批次计算平均生产时间
        batch_times = []
        for batch in self.material_df['批次号'].dropna().unique():
            batch_data = self.material_df[self.material_df['批次号'] == batch]
            if len(batch_data) > 0:
                start_time = pd.to_datetime(batch_data['日期'].min())
                end_time = pd.to_datetime(batch_data['日期'].max())
                if pd.notna(start_time) and pd.notna(end_time):
                    cycle_time = (end_time - start_time).total_seconds() / 3600
                    if cycle_time > 0:  # 只添加有效的时间
                        batch_times.append(cycle_time)
        
        return np.mean(batch_times) if batch_times else 0
    
    def analyze_time_efficiency(self):
        """时间维度分析"""
        # 计划与实际时间对比
        planned_time = self.equipment_df[self.equipment_df['状态'] == '运行']['总运行时间'].fillna(0).sum()
        actual_time = self.operation_df['操作时长'].fillna(0).sum()
        
        # 停机时间分析
        downtime_by_type = self.equipment_df[self.equipment_df['状态'].isin(['故障', '停机'])].groupby('状态')['故障持续时间'].sum().fillna(0)
        
        # 生产节拍分析
        total_products = self.material_df['产品数量'].fillna(0).sum()
        total_time = actual_time
        takt_time = total_time / total_products if total_products > 0 else 0
        
        return {
            '计划生产时间': planned_time,
            '实际生产时间': actual_time,
            '停机时间分布': downtime_by_type.to_dict(),
            '生产节拍时间': takt_time
        }
    
    def analyze_quality(self):
        """质量维度分析"""
        # 一次合格率
        total_products = self.material_df['产品数量'].fillna(0).sum()
        good_products = self.material_df['合格产品数量'].fillna(0).sum()
        first_pass_yield = good_products / total_products if total_products > 0 else 0
        
        # 报废率
        scrap_rate = (total_products - good_products) / total_products if total_products > 0 else 0
        
        # 质量损失（不合格品数量）
        quality_loss = total_products - good_products
        
        return {
            '一次合格率': first_pass_yield,
            '报废率': scrap_rate,
            '质量损失': quality_loss
        }
    
    def analyze_resource_utilization(self):
        """资源利用分析"""
        # 物料利用率
        material_input = self.material_df['物料投入量'].fillna(0).sum()
        material_used = self.material_df['物料使用量'].fillna(0).sum()
        material_efficiency = material_used / material_input if material_input > 0 else 0
        
        # 人力利用率
        total_work_hours = self.operation_df['操作时长'].fillna(0).sum()
        effective_work_hours = self.operation_df[
            self.operation_df['质检结果'].isin(['合格', 'PASS', 'OK'])
        ]['操作时长'].fillna(0).sum()
        labor_efficiency = effective_work_hours / total_work_hours if total_work_hours > 0 else 0
        
        return {
            '物料利用率': material_efficiency,
            '人力利用率': labor_efficiency
        }
    
    def generate_full_report(self):
        """生成完整的效率分析报告"""
        oee_metrics = self.calculate_oee()
        
        report = {
            '基础效率指标': {
                'OEE': oee_metrics['OEE'],
                '可用性': oee_metrics['可用性'],
                '性能效率': oee_metrics['性能效率'],
                '质量率': oee_metrics['质量率'],
                'TEEP': self.calculate_teep(),
                '产能利用率': self.calculate_capacity_utilization(),
                '平均生产周期时间': self.calculate_cycle_time()
            }
        }
        
        # 添加时间维度分析
        time_metrics = self.analyze_time_efficiency()
        report['时间维度分析'] = {
            '计划生产时间': time_metrics['计划生产时间'],
            '实际生产时间': time_metrics['实际生产时间'],
            '停机时间_停机': time_metrics['停机时间分布'].get('停机', 0),
            '停机时间_故障': time_metrics['停机时间分布'].get('故障', 0),
            '生产节拍时间': time_metrics['生产节拍时间']
        }
        
        # 添加质量维度分析
        quality_metrics = self.analyze_quality()
        report['质量维度分析'] = quality_metrics
        
        # 添加资源利用分析
        resource_metrics = self.analyze_resource_utilization()
        report['资源利用分析'] = resource_metrics
        
        return report
    
    def save_report(self, report, output_dir='reports'):
        """保存报告到CSV和Excel文件"""
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 将嵌套字典转换为扁平化的字典
        flat_report = {}
        for category, metrics in report.items():
            if isinstance(metrics, dict):
                for metric_name, value in metrics.items():
                    if isinstance(value, dict):
                        for sub_name, sub_value in value.items():
                            flat_report[f"{category}_{metric_name}_{sub_name}"] = sub_value
                    else:
                        flat_report[f"{category}_{metric_name}"] = value
            else:
                flat_report[category] = metrics
        
        # 转换为DataFrame
        df = pd.DataFrame([flat_report])
        
        # 保存为CSV
        csv_path = os.path.join(output_dir, 'efficiency_report.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        # 保存为Excel
        excel_path = os.path.join(output_dir, 'efficiency_report.xlsx')
        df.to_excel(excel_path, index=False, engine='openpyxl')
        
        return csv_path, excel_path

if __name__ == '__main__':
    try:
        # 读取处理后的数据
        data_path = 'data/processed_test_data.xlsx'
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"找不到数据文件: {data_path}")
        
        equipment_data = pd.read_excel(data_path, sheet_name='设备数据')
        operation_data = pd.read_excel(data_path, sheet_name='人员操作数据')
        material_data = pd.read_excel(data_path, sheet_name='物料数据')
        environment_data = pd.read_excel(data_path, sheet_name='环境数据')
        
        # 创建分析器实例
        analyzer = EfficiencyAnalyzer(equipment_data, operation_data, material_data, environment_data)
        
        # 生成报告
        report = analyzer.generate_full_report()
        
        # 保存报告
        csv_path, excel_path = analyzer.save_report(report)
        
        print("\n=== 生产效率分析报告 ===")
        print(f"\n报告已保存至:")
        print(f"CSV文件: {csv_path}")
        print(f"Excel文件: {excel_path}")
        
        # 打印主要指标（确保以百分比形式显示）
        print("\n主要指标:")
        print(f"OEE: {report['基础效率指标']['OEE'] * 100:.2f}%")
        print(f"TEEP: {report['基础效率指标']['TEEP'] * 100:.2f}%")
        print(f"产能利用率: {report['基础效率指标']['产能利用率'] * 100:.2f}%")
        print(f"一次合格率: {report['质量维度分析']['一次合格率'] * 100:.2f}%")
        print(f"物料利用率: {report['资源利用分析']['物料利用率'] * 100:.2f}%")
        print(f"人力利用率: {report['资源利用分析']['人力利用率'] * 100:.2f}%")
        
    except Exception as e:
        print(f"\n分析过程中出错: {str(e)}")
        raise 