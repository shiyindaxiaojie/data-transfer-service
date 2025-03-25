import os
from datetime import datetime
import pandas as pd
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv
from openpyxl import Workbook

# 初始配置
load_dotenv()

headers = {
    "PRIVATE-TOKEN": os.getenv("ACCESS_TOKEN")
}
current_year = datetime.now().year
path = f'{current_year}年度腾讯工蜂Git备份清单.xlsx'

# API URL
API_URL = "https://git.code.tencent.com/api/v3/"

def log(message):
    """简单的日志打印函数"""
    print(f"[{datetime.now()}]: {message}")


# 根据项目组或项目描述匹配开发负责人
def assign_developer(desc):
    conditions = {
        "恒生|作废|核心系统|资金|支付|自研项目": "顿邵坤",
        "普益基金|益起投|普益财富|普益投|Kettle|直播平台|私募基金": "苏坚昭",
        "普益商学|理财师|家|普益云服|基础平台|人工智能|运营终端|企业微信": "郭远陆",
        "机构通|投顾|交易|清算": "张志强",
        "数据部": "陈燕",
        "前端APP": "曾庆通",
        "投研系统|风险量化": "李儒记",
    }
    for condition, developer in conditions.items():
        if any(keyword in desc for keyword in condition.split("|")):
            log(f"分配给项目描述 '{desc}' 的开发负责人: {developer}")
            return developer
    log(f"未找到匹配的开发负责人条件，项目描述: '{desc}'")
    return ""


# 判断是否需要备份
def need_backup(description):
    result = "否" if "恒生" in description or "作废" in description else "是"
    log(f"项目描述 '{description}' 需要备份: {result}")
    return result


# 判断备份状态
def backup_status(description):
    status = "未备份" if "恒生" in description or "作废" in description else "已备份"
    log(f"项目描述 '{description}' 的备份状态: {status}")
    return status

# 构造备份路径
def backup_path(description, project_name):
    special_conditions = {
        "核心系统|资金|支付|自研项目|普益基金|普益财富|直播平台|私募基金|机构通|投顾|交易|清算|运营终端": "puyifund-prod",
        "基础平台|人工智能": "platform",
        "普益云服|益起投|普益投|Kettle|数据部|投研系统|风险量化": "others",
        "前端APP": "frontend",
        "普益商学": "bizcollege-prod",
        "理财师": "iplanner-prod",
        "企业微信": "iplanner-prod",
        "家办|家族办公室": "fois-prod"
    }

    for condition, domain in special_conditions.items():
        if any(keyword in description for keyword in condition.split("|")):
            path = f"https://e.coding.net/puyifund/{domain}/{project_name}.git"
            log(f"项目 '{project_name}' 的备份路径: {path}")
            return path

    # 如果没有匹配到任何条件，返回 N/A
    log(f"项目 '{project_name}' 未匹配到特定备份路径条件，备份路径设为 N/A")
    return "N/A"

# 使用curl_cffi获取所有项目组
def fetch_groups():
    log("开始获取所有项目组...")
    params = {
        "page": 1,
        "per_page": 100
    }
    response = cffi_requests.get(f"{API_URL}groups", params=params, headers=HEADERS, verify=True)
    response.raise_for_status()
    log("项目组获取成功")
    return response.json()


# 使用curl_cffi获取单个项目组详情
def fetch_group_details(group_id):
    log(f"开始获取项目组ID {group_id} 的详情...")
    response = cffi_requests.get(f"{API_URL}groups/{group_id}", headers=HEADERS, verify=True)
    response.raise_for_status()
    log(f"项目组ID {group_id} 详情获取成功")
    return response.json()


# 处理数据并导出到Excel
def export_to_excel(groups):
    log("开始处理数据并导出到Excel...")
    data = []
    for group in groups:
        group_detail = fetch_group_details(group['id'])
        for project in group_detail.get('projects', []):
            # 获取项目信息
            developer = assign_developer(group_detail['description'] + project['description'])
            backup_needed = need_backup(group_detail['description'])
            status = backup_status(group_detail['description'])
            backup_url = backup_path(group_detail['description'], project['name'])
            project_info = {
                '项目组': group['name'],
                '项目组描述': group_detail['description'],
                '项目': project['name'],
                '项目描述': project['description'],
                '项目地址': project['ssh_url_to_repo'],
                '开发负责人': developer,
                '是否需要备份': backup_needed,
                '备份状态': status,
                '备份路径': backup_url,
            }
            data.append(project_info)

    # 将数据转换为DataFrame
    df = pd.DataFrame(data)

    # 使用pandas写入Excel，设置引擎为openpyxl以支持列宽调整
    with pd.ExcelWriter(PATH, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')

        # 设置列宽
        ws = writer.sheets['Sheet1']
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 20
        ws.column_dimensions['C'].width = 20
        ws.column_dimensions['D'].width = 30
        ws.column_dimensions['E'].width = 70
        ws.column_dimensions['F'].width = 10
        ws.column_dimensions['G'].width = 15
        ws.column_dimensions['H'].width = 10
        ws.column_dimensions['I'].width = 70

    log(f"数据已成功导出至Excel: {PATH}")


if __name__ == '__main__':
    log("主程序开始执行...")
    all_groups = fetch_groups()
    log("获取到的项目组列表:")
    for group in all_groups:
        log(f"- {group['name']}")
    export_to_excel(all_groups)
    log("程序执行完成.")