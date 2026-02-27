import requests
import json

def test_apis():
    base_url = "http://localhost:5000/api"
    data = {
        "start_time": "2024-03-01",
        "end_time": "2024-03-20"
    }
    headers = {"Content-Type": "application/json"}
    
    # 测试效率分析API
    test_api("效率分析API", f"{base_url}/efficiency", data, headers)
    
    # 测试时间分析API
    test_api("时间分析API", f"{base_url}/time-analysis", data, headers)
    
    # 测试质量分析API
    test_api("质量分析API", f"{base_url}/quality-analysis", data, headers)
    
    # 测试资源分析API
    test_api("资源分析API", f"{base_url}/resource-analysis", data, headers)
    
    # 测试瓶颈分析API
    test_api("瓶颈分析API", f"{base_url}/bottleneck-analysis", data, headers)
    
    # 测试人员效能分析API
    test_api("人员效能分析API", f"{base_url}/staff-efficiency", data, headers)

def test_api(name, url, data, headers):
    print(f"\n测试 {name}:")
    try:
        response = requests.post(url, json=data, headers=headers)
        print(f"状态码: {response.status_code}")
        print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)[:300]}...")
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    test_apis() 