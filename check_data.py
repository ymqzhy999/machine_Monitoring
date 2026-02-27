import pandas as pd

def check_excel_data():
    try:
        # 读取Excel文件
        excel_file = 'data/test_data.xlsx'
        xls = pd.ExcelFile(excel_file)
        
        # 打印所有sheet名称
        print("Excel sheets:", xls.sheet_names)
        
        # 读取并检查每个sheet
        for sheet_name in xls.sheet_names:
            print(f"\n检查 {sheet_name} sheet:")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            print(f"列名: {df.columns.tolist()}")
            print(f"数据行数: {len(df)}")
            print("前5行数据:")
            print(df.head())
            
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    check_excel_data() 