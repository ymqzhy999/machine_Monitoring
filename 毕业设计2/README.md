# 智能制造生产效率分析系统

本系统用于分析智能制造生产线的效率指标，包含数据生成、数据处理和效率分析三个主要模块。

## 系统架构

1. 数据生成器 (data_generator.py)
   - 生成模拟测试数据
   - 包含设备、人员操作、物料和环境数据
   - 输出数据保存至 test_data.xlsx

2. 数据处理器 (data_processor.py)
   - 数据清洗和预处理
   - 统一数据格式
   - 处理异常值和缺失值
   - 智能数据填充

3. 效率分析器 (efficiency_analyzer.py)
   - 计算关键效率指标（OEE、TEEP等）
   - 多维度效率分析
   - 生成分析报告

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

1. 生成测试数据：
```bash
python data_generator.py
```

2. 处理数据：
```bash
python data_processor.py
```

3. 分析效率：
```bash
python efficiency_analyzer.py
```

## 输出说明

- test_data.xlsx：原始测试数据
- processed_data.xlsx：处理后的数据
- efficiency_report.txt：效率分析报告

## 主要效率指标

1. OEE (Overall Equipment Effectiveness)
2. TEEP (Total Effective Equipment Performance)
3. 劳动生产率
4. 产品合格率
5. 综合效率指标（设备60%、人员25%、环境15%） 