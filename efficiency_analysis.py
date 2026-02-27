import pandas as pd
from datetime import datetime, timedelta

class EfficiencyAnalyzer:
    def __init__(self, equipment_data, material_data, operation_data, environment_data):
        """初始化效率分析器
        
        Args:
            equipment_data (DataFrame): 设备数据
            material_data (DataFrame): 物料数据
            operation_data (DataFrame): 人员操作数据
            environment_data (DataFrame): 环境数据
        """
        self.equipment_df = equipment_data
        self.material_df = material_data
        self.operation_df = operation_data
        self.environment_df = environment_data

    def calculate_oee(self, device_id, start_time, end_time):
        """计算设备综合效率(OEE)
        OEE = 可用率 × 性能率 × 质量率
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含OEE及其组成部分的字典
        """
        # 获取时间范围内的数据
        mask = (self.equipment_df['时间戳'] >= start_time) & \
               (self.equipment_df['时间戳'] <= end_time) & \
               (self.equipment_df['设备ID'] == device_id)
        device_data = self.equipment_df[mask]
        
        # 1. 计算可用率 = 实际运行时间 / 计划运行时间
        total_time = (end_time - start_time).total_seconds() / 3600  # 转换为小时
        downtime = device_data[device_data['状态'].isin(['故障', '故障中', '停机'])]['故障持续时间'].sum()
        available_time = total_time - downtime
        availability = available_time / total_time if total_time > 0 else 0

        # 2. 计算性能率 = 实际产出 / 理论产出
        actual_output = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date()) &
            (self.material_df['物料编号'] == device_id)
        ]['产品数量'].sum()
        
        theoretical_output = available_time * 60  # 假设每分钟1个产品
        performance = actual_output / theoretical_output if theoretical_output > 0 else 0

        # 3. 计算质量率 = 合格品数量 / 总产出
        good_output = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date()) &
            (self.material_df['物料编号'] == device_id)
        ]['合格产品数量'].sum()
        
        quality = good_output / actual_output if actual_output > 0 else 0

        # 计算OEE
        oee = availability * performance * quality

        return {
            'OEE': oee,
            'availability': availability,
            'performance': performance,
            'quality': quality
        }

    def calculate_teep(self, device_id, start_time, end_time):
        """计算整体设备效率(TEEP)
        TEEP = OEE × 计划利用率
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含TEEP及其组成部分的字典
        """
        oee_metrics = self.calculate_oee(device_id, start_time, end_time)
        
        # 计算计划利用率 = 计划运行时间 / 日历时间
        calendar_time = (end_time - start_time).total_seconds() / 3600
        planned_downtime = self.equipment_df[
            (self.equipment_df['时间戳'] >= start_time) &
            (self.equipment_df['时间戳'] <= end_time) &
            (self.equipment_df['设备ID'] == device_id) &
            (self.equipment_df['状态'] == '计划停机')
        ]['总运行时间'].sum()
        
        planned_utilization = (calendar_time - planned_downtime) / calendar_time if calendar_time > 0 else 0
        teep = oee_metrics['OEE'] * planned_utilization

        return {
            'TEEP': teep,
            'planned_utilization': planned_utilization,
            'OEE': oee_metrics['OEE']
        }

    def analyze_productivity(self, start_time, end_time):
        """分析劳动生产率
        
        Args:
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含生产率指标的字典
        """
        # 获取时间范围内的数据
        operation_data = self.operation_df[
            (self.operation_df['时间戳'] >= start_time) &
            (self.operation_df['时间戳'] <= end_time)
        ]
        
        material_data = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date())
        ]

        # 计算总产出
        total_output = material_data['产品数量'].sum()
        
        # 计算工作人时
        total_work_hours = operation_data['操作时长'].sum()
        
        # 计算人均产出
        worker_count = operation_data['工号'].nunique()
        output_per_worker = total_output / worker_count if worker_count > 0 else 0
        
        # 计算每小时产出
        hourly_output = total_output / total_work_hours if total_work_hours > 0 else 0

        return {
            'total_output': total_output,
            'total_work_hours': total_work_hours,
            'worker_count': worker_count,
            'output_per_worker': output_per_worker,
            'hourly_output': hourly_output
        }

    def analyze_cycle_time(self, device_id, start_time, end_time):
        """分析生产周期时间
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含周期时间指标的字典
        """
        # 获取设备数据
        device_data = self.equipment_df[
            (self.equipment_df['时间戳'] >= start_time) &
            (self.equipment_df['时间戳'] <= end_time) &
            (self.equipment_df['设备ID'] == device_id)
        ]

        # 获取物料数据
        material_data = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date()) &
            (self.material_df['物料编号'] == device_id)
        ]

        # 计算实际运行时间
        running_time = device_data[device_data['状态'].isin(['运行', '运行中'])]['总运行时间'].sum()
        
        # 计算总产出
        total_output = material_data['产品数量'].sum()
        
        # 计算平均周期时间（小时/件）
        cycle_time = running_time / total_output if total_output > 0 else 0
        
        # 计算生产节拍（件/小时）
        takt_time = total_output / running_time if running_time > 0 else 0

        return {
            'cycle_time': cycle_time,
            'takt_time': takt_time,
            'total_output': total_output,
            'running_time': running_time
        }

    def analyze_downtime(self, device_id, start_time, end_time):
        """分析停机时间
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含停机分析的字典
        """
        device_data = self.equipment_df[
            (self.equipment_df['时间戳'] >= start_time) &
            (self.equipment_df['时间戳'] <= end_time) &
            (self.equipment_df['设备ID'] == device_id)
        ]

        # 计算各类停机时间
        fault_time = device_data[device_data['状态'].isin(['故障', '故障中'])]['故障持续时间'].sum()
        maintenance_time = device_data[device_data['状态'] == '维护']['总运行时间'].sum()
        idle_time = device_data[device_data['状态'] == '待机']['总运行时间'].sum()
        
        # 统计故障类型
        fault_types = device_data[device_data['状态'].isin(['故障', '故障中'])]['故障代码'].value_counts().to_dict()

        return {
            'total_downtime': fault_time + maintenance_time + idle_time,
            'fault_time': fault_time,
            'maintenance_time': maintenance_time,
            'idle_time': idle_time,
            'fault_types': fault_types
        }

    def analyze_quality(self, device_id, start_time, end_time):
        """分析质量指标
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含质量分析指标的字典
        """
        material_data = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date()) &
            (self.material_df['物料编号'] == device_id)
        ]

        # 计算总产出和合格品数量
        total_output = material_data['产品数量'].sum()
        good_output = material_data['合格产品数量'].sum()
        
        # 计算一次合格率(FTT)
        ftt = good_output / total_output if total_output > 0 else 0
        
        # 计算报废率
        scrap_rate = (total_output - good_output) / total_output if total_output > 0 else 0

        return {
            'total_output': total_output,
            'good_output': good_output,
            'ftt': ftt,
            'scrap_rate': scrap_rate
        }

    def analyze_resource_utilization(self, device_id, start_time, end_time):
        """分析资源利用率
        
        Args:
            device_id (str): 设备ID
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间
            
        Returns:
            dict: 包含资源利用分析的字典
        """
        # 获取物料数据
        material_data = self.material_df[
            (self.material_df['日期'].dt.date >= start_time.date()) &
            (self.material_df['日期'].dt.date <= end_time.date()) &
            (self.material_df['物料编号'] == device_id)
        ]

        # 计算物料利用率
        material_input = material_data['物料投入量'].sum()
        material_used = material_data['物料使用量'].sum()
        material_utilization = material_used / material_input if material_input > 0 else 0

        # 获取人员数据
        operation_data = self.operation_df[
            (self.operation_df['时间戳'] >= start_time) &
            (self.operation_df['时间戳'] <= end_time)
        ]

        # 计算人力利用率
        total_time = (end_time - start_time).total_seconds() / 3600
        actual_work_time = operation_data['操作时长'].sum()
        labor_utilization = actual_work_time / (total_time * operation_data['工号'].nunique()) if total_time > 0 else 0

        return {
            'material_utilization': material_utilization,
            'material_input': material_input,
            'material_used': material_used,
            'labor_utilization': labor_utilization,
            'total_work_hours': actual_work_time
        }

if __name__ == "__main__":
    # 读取数据
    equipment_data = pd.read_excel('data/processed_test_data.xlsx', sheet_name='设备数据')
    material_data = pd.read_excel('data/processed_test_data.xlsx', sheet_name='物料数据')
    operation_data = pd.read_excel('data/processed_test_data.xlsx', sheet_name='人员操作数据')
    environment_data = pd.read_excel('data/processed_test_data.xlsx', sheet_name='环境数据')

    # 创建分析器
    analyzer = EfficiencyAnalyzer(equipment_data, material_data, operation_data, environment_data)

    # 设置分析时间范围
    start_time = equipment_data['时间戳'].min()
    end_time = equipment_data['时间戳'].max()
    device_id = 'CNC001'

    # 1. 分析OEE
    oee_results = analyzer.calculate_oee(device_id, start_time, end_time)
    print("\n=== OEE分析结果 ===")
    print(f"OEE: {oee_results['OEE']:.2%}")
    print(f"可用率: {oee_results['availability']:.2%}")
    print(f"性能率: {oee_results['performance']:.2%}")
    print(f"质量率: {oee_results['quality']:.2%}")

    # 2. 分析TEEP
    teep_results = analyzer.calculate_teep(device_id, start_time, end_time)
    print("\n=== TEEP分析结果 ===")
    print(f"TEEP: {teep_results['TEEP']:.2%}")
    print(f"计划利用率: {teep_results['planned_utilization']:.2%}")

    # 3. 分析生产率
    productivity_results = analyzer.analyze_productivity(start_time, end_time)
    print("\n=== 生产率分析结果 ===")
    print(f"总产出: {productivity_results['total_output']:.0f}件")
    print(f"人均产出: {productivity_results['output_per_worker']:.2f}件/人")
    print(f"每小时产出: {productivity_results['hourly_output']:.2f}件/小时")

    # 4. 分析周期时间
    cycle_time_results = analyzer.analyze_cycle_time(device_id, start_time, end_time)
    print("\n=== 周期时间分析结果 ===")
    print(f"平均周期时间: {cycle_time_results['cycle_time']:.2f}小时/件")
    print(f"生产节拍: {cycle_time_results['takt_time']:.2f}件/小时")

    # 5. 分析停机时间
    downtime_results = analyzer.analyze_downtime(device_id, start_time, end_time)
    print("\n=== 停机时间分析结果 ===")
    print(f"总停机时间: {downtime_results['total_downtime']:.2f}小时")
    print(f"故障时间: {downtime_results['fault_time']:.2f}小时")
    print(f"维护时间: {downtime_results['maintenance_time']:.2f}小时")
    print(f"待机时间: {downtime_results['idle_time']:.2f}小时")

    # 6. 分析质量指标
    quality_results = analyzer.analyze_quality(device_id, start_time, end_time)
    print("\n=== 质量分析结果 ===")
    print(f"一次合格率(FTT): {quality_results['ftt']:.2%}")
    print(f"报废率: {quality_results['scrap_rate']:.2%}")

    # 7. 分析资源利用率
    resource_results = analyzer.analyze_resource_utilization(device_id, start_time, end_time)
    print("\n=== 资源利用分析结果 ===")
    print(f"物料利用率: {resource_results['material_utilization']:.2%}")
    print(f"人力利用率: {resource_results['labor_utilization']:.2%}") 