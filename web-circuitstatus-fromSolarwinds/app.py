import requests
import re
import html
import json
import xml.etree.ElementTree as ET
from ftfy import fix_text
import io
import csv
import datetime
import os
import argparse
import pandas as pd
from flask import Flask, request, render_template, jsonify, send_from_directory
import tempfile
import threading
import uuid
from reportlab.lib.pagesizes import letter, landscape # เพิ่ม landscape เข้ามา
from reportlab.platypus import SimpleDocTemplate, Table, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import logging
from queue import Queue
import zipfile
import shutil
import time

app = Flask(__name__)

# --- สถานะการประมวลผลและ Lock สำหรับ Thread-safe ---
processing_status = {}
status_lock = threading.Lock()

# --- ตั้งค่า Logger และ Log Queue ---
log_queue = Queue()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if logger.handlers:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

class QueueHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            msg = self.format(record)
            
            # ตรวจสอบและกรองข้อความที่ไม่ต้องการออก
            if "📁 ลบโฟลเดอร์ CSV/PDF ชั่วคราว" in msg:
                return # ไม่ใส่ข้อความนี้ลงในคิว

            log_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - (INFO|WARNING|ERROR|CRITICAL) - (Job [0-9a-f-]+: )?(.*)")
            match = log_pattern.match(msg)
            if match:
                clean_msg = match.group(3)
                self.queue.put(clean_msg)
            else:
                self.queue.put(msg)

        except Exception:
            self.handleError(record)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

queue_handler = QueueHandler(log_queue)
logger.addHandler(queue_handler)

# --- ตั้งค่าฟอนต์ภาษาไทยสำหรับ PDF ---
THAI_FONT_NAME = 'THSarabunNew'
THAI_FONT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'THSarabunNew.ttf')

THAI_FONT_REGISTERED = False
if os.path.exists(THAI_FONT_PATH):
    try:
        pdfmetrics.registerFont(TTFont(THAI_FONT_NAME, THAI_FONT_PATH))
        THAI_FONT_REGISTERED = True
        logger.info(f"Thai font '{THAI_FONT_NAME}' registered successfully from '{THAI_FONT_PATH}'.")
    except Exception as e:
        logger.error(f"ERROR: Could not register Thai font '{THAI_FONT_NAME}'. Error: {e}")
else:
    logger.warning(f"WARNING: Thai font file '{THAI_FONT_PATH}' not found. Please ensure the font file is in the same directory as the script.")

# --- ฟังก์ชันสำหรับประมวลผลข้อมูล ---
def get_data_from_api(nod_id, itf_id, job_id):
    """ดึงข้อมูลจาก API และแปลงเป็น JSON"""
    url = "http://1.179.233.116:8082/api_csoc_02/server_solarwinds_gin.php" 
    headers = {
        # เปลี่ยน IP Address ตรงนี้ใน SOAPAction ด้วย
        "SOAPAction": "http://1.179.233.116/api_csoc_02/server_solarwinds_gin.php/circuitStatus",
        "Content-Type": "text/xml; charset=utf-8",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <circuitStatus xmlns="http://1.179.233.116/soap/#Service_Solarwinds_gin"> <nodID>{nod_id}</nodID>
      <itfID>{itf_id}</itfID>
    </circuitStatus>
  </soap:Body>
</soap:Envelope>"""

    try:
        resp = requests.post(url, data=body, headers=headers, timeout=10)
        resp.raise_for_status()
        match = re.search(r"(<\?xml.*?</SOAP-ENV:Envelope>)", resp.text, re.DOTALL)
        if not match:
            logger.warning(f"ไม่พบ XML Response สำหรับ NodeID: {nod_id}, Interface ID: {itf_id}")
            return None
        
        root = ET.fromstring(match.group(1))
        return_tag = root.find(".//{*}return")
        if return_tag is None or not return_tag.text:
            logger.warning(f"API ไม่มีข้อมูลตอบกลับสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}")
            return None

        raw_text = return_tag.text
        html_unescaped = html.unescape(raw_text)
        fixed_text = fix_text(bytes(html_unescaped, "utf-8").decode("unicode_escape"))
        parsed_json = json.loads(fixed_text)
        return parsed_json
    except requests.exceptions.RequestException as req_e:
        logger.error(f"❌ ดึงข้อมูล NodeID: {nod_id}, Interface ID: {itf_id} ล้มเหลว: {req_e}")
        return None
    except ET.ParseError as parse_e:
        logger.error(f"❌ XML Parsing ผิดพลาดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {parse_e}")
        return None
    except json.JSONDecodeError as json_e:
        logger.error(f"❌ JSON Decoding ผิดพลาดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {json_e}")
        return None
    except Exception as e:
        logger.error(f"❌ ข้อผิดพลาดไม่คาดคิดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {e}")
        return None

def process_json_data(raw_json_data, job_id, excel_node_id, excel_agency_name):
    """
    ประมวลผลข้อมูล JSON ที่ได้จาก API เพื่อเตรียมสำหรับสร้างไฟล์ CSV/PDF
    - เติมข้อมูลให้ครบ 24 ชั่วโมงในแต่ละวัน
    - คำนวณค่าเฉลี่ยรวม (Grand Total Average)
    - จัดรูปแบบ Bandwidth และปริมาณการใช้งาน

    Parameters:
    - raw_json_data (list or dict): ข้อมูล JSON ดิบจาก API
    - job_id (str): ID ของงานปัจจุบันสำหรับ logging
    - excel_node_id (str): Node ID จากไฟล์ Excel (ใช้เป็นค่าเริ่มต้นหาก API ไม่มี)
    - excel_agency_name (str): ชื่อหน่วยงานจากไฟล์ Excel (ใช้เป็นค่าเริ่มต้นหาก API ไม่มี)

    Returns:
    - tuple: (headers, processed_data, grand_total_row)
        - headers (list): รายชื่อหัวข้อคอลัมน์ภาษาไทย
        - processed_data (list): ข้อมูลที่ประมวลผลแล้วพร้อมสำหรับตาราง
        - grand_total_row (dict): แถว Grand Total (Average)
    """
    column_mapping = {
        "รหัสหน่วยงาน": "Customer_Curcuit_ID",
        "ชื่อหน่วยงาน": "Address",
        "วันที่และเวลา": "Timestamp",
        "ขนาดBandwidth (หน่วย Mbps)": "Bandwidth",
        "In_Averagebps": "In_Averagebps",
        "Out_Averagebps": "Out_Averagebps"
    }
    desired_headers_th = list(column_mapping.keys())
    
    # ตรวจสอบว่าข้อมูลเป็น list หรือ dict แล้วแปลงให้เป็น list เสมอ
    data_to_process = raw_json_data if isinstance(raw_json_data, list) else [raw_json_data]

    if not data_to_process:
        logger.warning(f"Job {job_id}: ไม่มีข้อมูล JSON ให้ประมวลผล")
        return desired_headers_th, [], {}

    # กำหนดค่าเริ่มต้นสำหรับข้อมูลหลัก (รหัสหน่วยงาน, ชื่อหน่วยงาน, Bandwidth)
    # ค่าเหล่านี้จะถูกใช้เป็นค่าตั้งต้นสำหรับหัวเรื่องใน PDF และถูกเติมลงในแต่ละแถวข้อมูล
    # หากข้อมูลจาก API ไม่มีค่าเหล่านี้
    #customer_circuit_id_to_use = excel_node_id # เริ่มต้นใช้จาก Excel
    address_to_use = excel_agency_name # เริ่มต้นใช้จาก Excel
    bandwidth_to_use = '' # ค่าเริ่มต้นว่างเปล่า จะถูกคำนวณจาก API หรือใช้ raw

    first_item = data_to_process[0]

    # พยายามดึงข้อมูลจาก API ก่อน หากมีค่า จะใช้ข้อมูลจาก API แทนค่าจาก Excel
    api_customer_circuit_id = first_item.get(column_mapping["รหัสหน่วยงาน"], '')
    api_address = first_item.get(column_mapping["ชื่อหน่วยงาน"], '')
    bandwidth_raw_from_api = first_item.get(column_mapping["ขนาดBandwidth (หน่วย Mbps)"], '')
    
    #if api_customer_circuit_id:
    #    customer_circuit_id_to_use = api_customer_circuit_id
    if api_address:
        address_to_use = api_address
    
    # ประมวลผล Bandwidth เพื่อให้แสดงผลในรูปแบบที่ต้องการ (เช่น "20 Mbps.")
    if "FTTx" in str(bandwidth_raw_from_api):
        bandwidth_to_use = "20 Mbps."
    else:
        try:
            numeric_value_match = re.search(r'[\d.]+', str(bandwidth_raw_from_api))
            if numeric_value_match:
                numeric_value = float(numeric_value_match.group())
                bandwidth_to_use = f"{int(numeric_value)} Mbps."
            else:
                bandwidth_to_use = str(bandwidth_raw_from_api) # ถ้าไม่พบตัวเลขก็ใช้ raw string
        except (ValueError, TypeError, AttributeError):
            bandwidth_to_use = str(bandwidth_raw_from_api) # ถ้าแปลงไม่ได้ก็ใช้ raw string


    # 1. กำหนดช่วงเวลาของรายงาน (เต็มเดือน)
    min_date_from_api = None
    max_date_from_api = None

    for item in data_to_process:
        if isinstance(item.get('Timestamp'), dict) and 'date' in item['Timestamp']:
            try:
                dt_obj_from_api = datetime.datetime.strptime(item['Timestamp']['date'], '%Y-%m-%d %H:%M:%S.%f')
                if min_date_from_api is None or dt_obj_from_api.date() < min_date_from_api:
                    min_date_from_api = dt_obj_from_api.date()
                if max_date_from_api is None or dt_obj_from_api.date() > max_date_from_api:
                    max_date_from_api = dt_obj_from_api.date()
            except ValueError:
                logger.warning(f"Job {job_id}: ไม่สามารถแปลงวันที่จาก Timestamp ในข้อมูล API ได้: {item.get('Timestamp')}")
                continue

    # หากไม่มีวันที่ที่ถูกต้องจาก API ให้ใช้เดือนปัจจุบันเป็นค่าเริ่มต้น
    if min_date_from_api is None or max_date_from_api is None:
        today = datetime.date.today()
        report_start_date = today.replace(day=1)
        # คำนวณวันสิ้นสุดของเดือนปัจจุบัน
        next_month_start = (today.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
        report_end_date = next_month_start - datetime.timedelta(days=1)
        logger.warning(f"Job {job_id}: ไม่พบ Timestamp ที่ถูกต้องในข้อมูล API, ใช้เดือนปัจจุบันเป็นวันอ้างอิง: {report_start_date} ถึง {report_end_date}.")
    else:
        # ขยายช่วงเวลาให้ครอบคลุมทั้งเดือนที่ข้อมูล API อยู่
        report_start_date = min_date_from_api.replace(day=1)
        # คำนวณวันสิ้นสุดของเดือนสุดท้ายที่มีข้อมูล
        next_month_start_for_max = (max_date_from_api.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
        report_end_date = next_month_start_for_max - datetime.timedelta(days=1)
        logger.info(f"Job {job_id}: กำหนดช่วงรายงานจากข้อมูล API: {report_start_date} ถึง {report_end_date}.")

    # สร้างโครงสร้างข้อมูลที่สมบูรณ์สำหรับทุกวันและทุกชั่วโมงในช่วงเวลาที่กำหนด
    full_data_structure = {}
    current_date = report_start_date
    while current_date <= report_end_date:
        for hour in range(24):
            dt_obj = datetime.datetime.combine(current_date, datetime.time(hour, 0, 0))
            formatted_date_time = dt_obj.strftime('%Y-%m-%d %H.%M.%S')
            
            date_key = current_date.strftime('%Y-%m-%d')
            if date_key not in full_data_structure:
                full_data_structure[date_key] = {}

            # เติมข้อมูล 24 ชั่วโมง โดยใช้ค่า customer_circuit_id_to_use, address_to_use, bandwidth_to_use
            # และกำหนดค่าเริ่มต้นของปริมาณการใช้งานเป็น "0"
            full_data_structure[date_key][formatted_date_time] = {
                "รหัสหน่วยงาน": api_customer_circuit_id, 
                "ชื่อหน่วยงาน": address_to_use,             
                "วันที่และเวลา": formatted_date_time,
                "ขนาดBandwidth (หน่วย Mbps)": bandwidth_to_use, 
                "In_Averagebps": "0",
                "Out_Averagebps": "0",
                "_raw_incoming": 0, # เก็บค่าดิบสำหรับคำนวณเฉลี่ย
                "_raw_outcoming": 0 # เก็บค่าดิบสำหรับคำนวณเฉลี่ย
            }
        current_date += datetime.timedelta(days=1) # เลื่อนไปยังวันถัดไป

    # อัปเดตข้อมูลจาก API ทับลงในโครงสร้างที่สร้างไว้
    for item in data_to_process:
        dt_obj_from_api = None
        if isinstance(item.get('Timestamp'), dict) and 'date' in item['Timestamp']:
            try:
                dt_obj_from_api = datetime.datetime.strptime(item['Timestamp']['date'], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                continue
        
        if dt_obj_from_api:
            date_key = dt_obj_from_api.strftime('%Y-%m-%d')
            formatted_time_from_api = dt_obj_from_api.strftime('%Y-%m-%d %H.%M.%S')
            
            if date_key in full_data_structure and formatted_time_from_api in full_data_structure[date_key]:
                in_avg_bps = item.get('In_Averagebps', '0')
                out_avg_bps = item.get('Out_Averagebps', '0')

                # แปลงและจัดรูปแบบIn_Averagebps
                try:
                    in_avg_bps_float = float(in_avg_bps)
                    full_data_structure[date_key][formatted_time_from_api]["_raw_incoming"] = int(in_avg_bps_float) 
                    full_data_structure[date_key][formatted_time_from_api]["In_Averagebps"] = f"{int(in_avg_bps_float):,}"
                except (ValueError, TypeError):
                    full_data_structure[date_key][formatted_time_from_api]["_raw_incoming"] = 0
                    full_data_structure[date_key][formatted_time_from_api]["In_Averagebps)"] = str(in_avg_bps)

                # แปลงและจัดรูปแบบOut_Averagebps
                try:
                    out_avg_bps_float = float(out_avg_bps)
                    full_data_structure[date_key][formatted_time_from_api]["_raw_outcoming"] = int(out_avg_bps_float)
                    full_data_structure[date_key][formatted_time_from_api]["Out_Averagebps"] = f"{int(out_avg_bps_float):,}"
                except (ValueError, TypeError):
                    full_data_structure[date_key][formatted_time_from_api]["_raw_outcoming"] = 0
                    full_data_structure[date_key][formatted_time_from_api]["Out_Averagebps"] = str(out_avg_bps)
                
                # อัปเดต "รหัสหน่วยงาน", "ชื่อหน่วยงาน" ด้วยค่าจาก API หากมี
                full_data_structure[date_key][formatted_time_from_api]["รหัสหน่วยงาน"] = item.get(column_mapping["รหัสหน่วยงาน"], api_customer_circuit_id)
                full_data_structure[date_key][formatted_time_from_api]["ชื่อหน่วยงาน"] = item.get(column_mapping["ชื่อหน่วยงาน"], address_to_use)
                
                # ประมวลผล Bandwidth จาก item โดยตรง หากมีค่าจาก API จะใช้ค่านั้น
                item_bandwidth_raw = item.get(column_mapping["ขนาดBandwidth (หน่วย Mbps)"], '')
                if item_bandwidth_raw:
                    if "FTTx" in str(item_bandwidth_raw):
                        full_data_structure[date_key][formatted_time_from_api]["ขนาดBandwidth (หน่วย Mbps)"] = "20 Mbps."
                    else:
                        try:
                            numeric_value_match = re.search(r'[\d.]+', str(item_bandwidth_raw))
                            if numeric_value_match:
                                numeric_value = float(numeric_value_match.group())
                                full_data_structure[date_key][formatted_time_from_api]["ขนาดBandwidth (หน่วย Mbps)"] = f"{int(numeric_value)} Mbps."
                            else:
                                full_data_structure[date_key][formatted_time_from_api]["ขนาดBandwidth (หน่วย Mbps)"] = str(item_bandwidth_raw)
                        except (ValueError, TypeError, AttributeError):
                            full_data_structure[date_key][formatted_time_from_api]["ขนาดBandwidth (หน่วย Mbps)"] = str(item_bandwidth_raw)
                else:
                    # หาก API ไม่มีค่า bandwidth สำหรับ item นี้ ให้ใช้ค่าที่คำนวณไว้ตอนต้น
                    full_data_structure[date_key][formatted_time_from_api]["ขนาดBandwidth (หน่วย Mbps)"] = bandwidth_to_use

    # รวบรวมข้อมูลที่ประมวลผลแล้วทั้งหมด และคำนวณ Grand Total
    processed_data = []
    total_incoming_sum = 0
    total_outcoming_sum = 0
    data_points_count = 0

    for date_key in sorted(full_data_structure.keys()):
        for time_key in sorted(full_data_structure[date_key].keys()):
            row = full_data_structure[date_key][time_key]
            processed_data.append(row)
            
            total_incoming_sum += row.get("_raw_incoming", 0)
            total_outcoming_sum += row.get("_raw_outcoming", 0)
            data_points_count += 1

    average_incoming = 0
    average_outcoming = 0
    if data_points_count > 0:
        average_incoming = round(total_incoming_sum / data_points_count)
        average_outcoming = round(total_outcoming_sum / data_points_count)

    # สร้างแถว Grand Total (Average)
    grand_total_row = {
        "รหัสหน่วยงาน": "Grand Total", 
        "ชื่อหน่วยงาน": "", 
        "วันที่และเวลา": "", 
        "ขนาดBandwidth (หน่วย Mbps)": "", 
        "In_Averagebps": f"{average_incoming:,}",
        "Out_Averagebps": f"{average_outcoming:,}"
    }

    return desired_headers_th, processed_data, grand_total_row

def export_to_csv(headers, data, filename, job_id, node_name):
    """สร้างและบันทึกไฟล์ CSV"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            cw = csv.writer(f)
            if headers and data:
                cw.writerow(headers)
                last_customer_id = None
                last_customer_name = None
                for row in data:
                    new_row = [
                        row.get('รหัสหน่วยงาน', ''),
                        row.get('ชื่อหน่วยงาน', ''),
                        row.get('วันที่และเวลา', ''),
                        row.get('ขนาดBandwidth (หน่วย Mbps)', ''),
                        row.get('In_Averagebps', ''),
                        row.get('Out_Averagebps', '')
                    ]
                    
                    cw.writerow(new_row)
            else:
                cw.writerow(["No Data"])
        logger.info(f"✅ สร้าง CSV สำหรับ '{node_name}' สำเร็จแล้ว")
        return True, "Success"
    except Exception as e:
        logger.error(f"❌ สร้าง CSV สำหรับ '{node_name}' ล้มเหลว: {e}")
        return False, str(e)

def export_to_pdf(headers, daily_data, grand_total_row, filename, job_id, node_name):
    """
    สร้างและบันทึกไฟล์ PDF โดยให้แต่ละวันขึ้นหน้าใหม่, Grand Total อยู่ต่อท้ายวันสุดท้าย
    ข้อมูล "รหัสหน่วยงาน" และ "ชื่อหน่วยงาน" จะแสดงเพียงครั้งเดียวต่อวัน
    """
    try:
        doc = SimpleDocTemplate(filename, pagesize=letter) 
        styles = getSampleStyleSheet()
        elements = []

        if headers and daily_data:
            data_by_date = {}
            # จัดกลุ่มข้อมูลตามวันที่
            for row in daily_data:
                date_time_str = row.get('วันที่และเวลา', '')
                try:
                    date_key = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H.%M.%S').strftime('%Y-%m-%d')
                except ValueError:
                    date_key = 'Uncategorized'
                    logger.warning(f"Job {job_id}: Found uncategorized date for PDF: {date_time_str}")
                if date_key not in data_by_date:
                    data_by_date[date_key] = []
                data_by_date[date_key].append(row)
            
            # --- กำหนดเดือนสำหรับ "รายงานประจำเดือน" โดยดึงจากข้อมูล ---
            report_month_str = "ไม่ระบุเดือน"
            if data_by_date:
                first_date_str = sorted(data_by_date.keys())[0]
                try:
                    first_date_obj = datetime.datetime.strptime(first_date_str, '%Y-%m-%d')
                    thai_months = {
                        1: "มกราคม", 2: "กุมภาพันธ์", 3: "มีนาคม", 4: "เมษายน",
                        5: "พฤษภาคม", 6: "มิถุนายน", 7: "กรกฎาคม", 8: "สิงหาคม",
                        9: "กันยายน", 10: "ตุลาคม", 11: "พฤศจิกายน", 12: "ธันวาคม"
                    }
                    report_month_str = thai_months.get(first_date_obj.month, "ไม่ระบุเดือน")
                except ValueError:
                    logger.warning(f"Job {job_id}: Could not parse first date for month determination: {first_date_str}")

            first_page = True
            sorted_dates = sorted(data_by_date.keys()) 
            
            for i, date_key in enumerate(sorted_dates): # วนลูปตามวันที่ที่จัดเรียงแล้ว
                group_data = data_by_date[date_key]

                if not first_page:
                    elements.append(PageBreak()) # ขึ้นหน้าใหม่สำหรับแต่ละวัน
                
                # ตั้งค่าสไตล์สำหรับหัวเรื่อง
                title_style = styles['Title']
                if THAI_FONT_REGISTERED:
                    title_style.fontName = THAI_FONT_NAME
                title_style.fontSize = 18
                title_style.alignment = 1 # จัดกึ่งกลาง
                elements.append(Paragraph("Customer Interface Summary Report by Hour", title_style))
                elements.append(Spacer(1, 0.2 * inch))

                # ตั้งค่าสไตล์สำหรับรายงานประจำเดือน
                month_report_style = ParagraphStyle('MonthReport', parent=styles['Normal'])
                if THAI_FONT_REGISTERED:
                    month_report_style.fontName = THAI_FONT_NAME
                month_report_style.fontSize = 14
                month_report_style.alignment = 1
                elements.append(Paragraph(f"รายงานประจำเดือน {report_month_str}", month_report_style))
                elements.append(Spacer(1, 0.2 * inch))

                # ตั้งค่าสไตล์สำหรับหัวข้อวันที่
                date_header_style = ParagraphStyle('DateHeader', parent=styles['Normal'])
                if THAI_FONT_REGISTERED:
                    date_header_style.fontName = THAI_FONT_NAME
                date_header_style.fontSize = 12
                date_header_style.alignment = 1
                elements.append(Paragraph(f"<b>วันที่ </b> {date_key}", date_header_style))
                elements.append(Spacer(1, 0.2 * inch))

                # กำหนดหัวตาราง
                table_headers = [
                    "รหัสหน่วยงาน",
                    "ชื่อหน่วยงาน",
                    "วันที่และเวลา",
                    "ขนาดBandwidth (หน่วย Mbps)",
                    "ปริมาณการใช้งาน incoming (หน่วย bps)",
                    "ปริมาณการใช้งาน outcoming (หน่วย bps)"
                ]
                
                table_data = [table_headers] # เพิ่มหัวตารางเข้าไปในข้อมูลตาราง

                last_customer_id = None
                last_customer_name = None
                
                for idx_row, row in enumerate(group_data):
                    current_customer_id = row.get('รหัสหน่วยงาน', '')
                    current_customer_name = row.get('ชื่อหน่วยงาน', '')
                    current_bandwidth = row.get('ขนาดBandwidth (หน่วย Mbps)', '')

                    display_customer_id = current_customer_id
                    display_customer_name = current_customer_name
                    display_bandwidth = current_bandwidth # Bandwidth จะแสดงทุกแถว

                    # ถ้าไม่ใช่แถวแรกของกลุ่ม และข้อมูลซ้ำกับแถวก่อนหน้า ให้เว้นว่าง
                    if idx_row > 0:
                        if current_customer_id == last_customer_id:
                            display_customer_id = ''
                        if current_customer_name == last_customer_name:
                            display_customer_name = ''

                    table_data.append([
                        display_customer_id,
                        display_customer_name,
                        row.get('วันที่และเวลา', ''),
                        display_bandwidth, # แสดง bandwidth เสมอ
                        row.get('In_Averagebps', ''),
                        row.get('Out_Averagebps', '')
                    ])
                    
                    # อัปเดตค่า last_ สำหรับการวนซ้ำครั้งถัดไป
                    last_customer_id = current_customer_id
                    last_customer_name = current_customer_name
                
                # หากเป็นวันสุดท้าย และมีแถว Grand Total ให้เพิ่มเข้าไปในตาราง
                if i == len(sorted_dates) - 1 and grand_total_row:
                    table_data.append([
                        grand_total_row.get('รหัสหน่วยงาน', ''),
                        grand_total_row.get('ชื่อหน่วยงาน', ''),
                        grand_total_row.get('วันที่และเวลา', ''),
                        grand_total_row.get('ขนาดBandwidth (หน่วย Mbps)', ''),
                        grand_total_row.get('In_Averagebps', ''),
                        grand_total_row.get('Out_Averagebps', '')
                    ])

                table = Table(table_data)
                # กำหนดสไตล์ของตาราง
                table_style = [
                    ('BACKGROUND', (0, 0), (-1, 0), '#cccccc'), # พื้นหลังหัวตาราง
                    ('TEXTCOLOR', (0, 0), (-1, 0), '#000000'), # สีตัวอักษรหัวตาราง
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'), # จัดกึ่งกลางทุกเซลล์
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12), # ระยะห่างด้านล่างของหัวตาราง
                    ('BACKGROUND', (0, 1), (-1, -1), "#ffffff"), # พื้นหลังข้อมูลตาราง
                    ('GRID', (0, 0), (-1, -1), 1, '#999999'), # เส้นกริด
                    ('FONTSIZE', (0, 0), (-1, -1), 10), # ขนาดฟอนต์
                    ('LEFTPADDING', (0,0), (-1,-1), 6), # ระยะห่างซ้าย
                    ('RIGHTPADDING', (0,0), (-1,-1), 6), # ระยะห่างขวา
                ]
                
                # กำหนดฟอนต์ภาษาไทยหากลงทะเบียนไว้
                if THAI_FONT_REGISTERED:
                    table_style.append(('FONTNAME', (0, 0), (-1, 0), THAI_FONT_NAME)) # ฟอนต์หัวตาราง
                    table_style.append(('FONTNAME', (0, 1), (-1, -1), THAI_FONT_NAME)) # ฟอนต์ข้อมูลตาราง
                else:
                    table_style.append(('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'))
                    table_style.append(('FONTNAME', (0, 1), (-1, -1), 'Helvetica'))
                
                # สไตล์เฉพาะสำหรับแถว Grand Total
                if i == len(sorted_dates) - 1 and grand_total_row:
                    grand_total_row_index = len(table_data) - 1
                    table_style.append(('BACKGROUND', (0, grand_total_row_index), (-1, grand_total_row_index), '#dddddd')) # พื้นหลัง
                    table_style.append(('FONTNAME', (0, grand_total_row_index), (-1, grand_total_row_index), THAI_FONT_NAME if THAI_FONT_REGISTERED else 'Helvetica-Bold')) # ฟอนต์
                    table_style.append(('SPAN', (0, grand_total_row_index), (3, grand_total_row_index))) # รวมเซลล์สำหรับ "Grand Total"
                    table_style.append(('ALIGN', (0, grand_total_row_index), (3, grand_total_row_index), 'LEFT')) # จัดชิดซ้าย
                    table_style.append(('VALIGN', (0, grand_total_row_index), (-1, grand_total_row_index), 'MIDDLE')) # จัดแนวตั้งกึ่งกลาง

                table.setStyle(table_style)
                elements.append(table)
                elements.append(Spacer(1, 0.5 * inch))

                first_page = False # ตั้งค่าเป็น False หลังจากหน้าแรก
            
        else:
            # กรณีไม่มีข้อมูล
            no_data_style = styles['Normal']
            if THAI_FONT_REGISTERED:
                no_data_style.fontName = THAI_FONT_NAME
            elements.append(Paragraph("No circuit status data available.", no_data_style))
        
        doc.build(elements) # สร้างเอกสาร PDF
        logger.info(f"✅ สร้าง PDF สำหรับ '{node_name}' สำเร็จแล้ว")
        return True, "PDF generated successfully."
    except Exception as e:
        logger.error(f"❌ สร้าง PDF สำหรับ '{node_name}' ล้มเหลว: {e}")
        return False, f"Error generating PDF: {e}"

def process_file_in_background(file_stream, job_id):
    """
    ฟังก์ชันนี้จะทำงานในอีก Thread หนึ่ง
    โดยจะรับ file_stream (ข้อมูลไฟล์) และ job_id มาประมวลผล
    """
    temp_dir = None
    try:
        df = pd.read_excel(file_stream)
        total_rows = len(df)
        with status_lock:
            processing_status[job_id]['total'] = total_rows
            processing_status[job_id]['results'] = []
            temp_dir = tempfile.mkdtemp(prefix=f"report_job_{job_id}_")
            processing_status[job_id]['temp_dir'] = temp_dir
        
        logger.info(f"📊 เริ่มประมวลผลไฟล์ Excel มีทั้งหมด {total_rows} รายการ")

        required_columns = ['NodeID', 'Interface ID', 'กระทรวง / สังกัด', 'กรม / สังกัด', 'จังหวัด', 'ชื่อหน่วยงาน', 'Node Name']
        if not all(col in df.columns for col in required_columns):
            missing_cols = [c for c in required_columns if c not in df.columns]
            with status_lock:
                processing_status[job_id]['error'] = f"ไฟล์ Excel ขาดคอลัมน์ที่จำเป็น: {', '.join(missing_cols)}"
                processing_status[job_id]['completed'] = True
            logger.error(f"❌ {processing_status[job_id]['error']}")
            return
        
        csv_root_dir = os.path.join(temp_dir, 'CSV')
        pdf_root_dir = os.path.join(temp_dir, 'PDF')
        os.makedirs(csv_root_dir, exist_ok=True)
        os.makedirs(pdf_root_dir, exist_ok=True)
        
        for index, row in df.iterrows():
            with status_lock:
                if processing_status[job_id].get('canceled'):
                    logger.info(f"⛔ งานถูกยกเลิกโดยผู้ใช้")
                    break
            
            node_name = ''
            csv_success = False
            pdf_success = False
            error_message = None

            try:
                nod_id = str(row['NodeID']).strip()
                itf_id = str(row['Interface ID']).strip()
                
                folder1 = str(row['กระทรวง / สังกัด']).strip()
                folder2 = str(row['กรม / สังกัด']).strip()
                folder3 = str(row['จังหวัด']).strip()
                folder4 = str(row['ชื่อหน่วยงาน']).strip()
                node_name = str(row['Node Name']).strip()

                if not nod_id or not itf_id:
                    error_message = "ข้อมูล NodeID หรือ Interface ID ไม่สมบูรณ์"
                    logger.warning(f"⚠️ ข้ามแถวที่ {index + 1} เนื่องจาก {error_message} (NodeID: '{nod_id}', ITF ID: '{itf_id}')")
                    with status_lock:
                        processing_status[job_id]['processed'] += 1
                        processing_status[job_id]['results'].append({
                            'node_name': node_name,
                            'csv_success': False,
                            'pdf_success': False,
                            'error_message': error_message
                        })
                    continue
                
                logger.info(f"▶ กำลังประมวลผล NodeID: {nod_id}, Interface ID: {itf_id} (แถวที่ {index + 1})")

                current_csv_dir = os.path.join(csv_root_dir, folder1, folder2, folder3, folder4)
                current_pdf_dir = os.path.join(pdf_root_dir, folder1, folder2, folder3, folder4)
                
                os.makedirs(current_csv_dir, exist_ok=True)
                os.makedirs(current_pdf_dir, exist_ok=True)
                
                raw_json_data = get_data_from_api(nod_id, itf_id, job_id)

                if raw_json_data:
                    headers, processed_daily_data, grand_total_row_data = process_json_data(raw_json_data, job_id, nod_id, folder4)
                    
                    sanitized_node_name = re.sub(r'[\\/:*?"<>|]', '_', node_name)
                    filename_base = f"{sanitized_node_name}" 

                    csv_filename = os.path.join(current_csv_dir, f"{filename_base}.csv")
                    pdf_filename = os.path.join(current_pdf_dir, f"{filename_base}.pdf")

                    # For CSV, append grand_total_row_data to processed_daily_data
                    csv_data_to_write = list(processed_daily_data) # Create a copy
                    if grand_total_row_data:
                        csv_data_to_write.append(grand_total_row_data)

                    csv_success, csv_msg = export_to_csv(headers, csv_data_to_write, csv_filename, job_id, node_name)
                    pdf_success, pdf_msg = export_to_pdf(headers, processed_daily_data, grand_total_row_data, pdf_filename, job_id, node_name)
                else:
                    error_message = f"ไม่สามารถดึงข้อมูลจาก API ได้สำหรับ NodeID: {nod_id}, Interface ID: {itf_id}"
                    logger.error(f"❌ {error_message}")
            
            except Exception as e:
                error_message = f"เกิดข้อผิดพลาดที่ไม่คาดคิดในแถวที่ {index + 1}: {e}"
                logger.error(f"❌ {error_message}")
                
            finally:
                with status_lock:
                    processing_status[job_id]['processed'] += 1
                    processing_status[job_id]['results'].append({
                        'node_name': node_name,
                        'csv_success': csv_success,
                        'pdf_success': pdf_success,
                        'error_message': error_message
                    })
        
        if not processing_status[job_id].get('canceled'):
            zip_filename = f"customer_reports_{job_id}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
            zip_file_path = os.path.join(tempfile.gettempdir(), zip_filename)
            
            if temp_dir and os.path.exists(temp_dir):
                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, temp_dir)
                            zipf.write(file_path, arcname)
                
                with status_lock:
                    processing_status[job_id]['zip_file_path'] = zip_file_path
                    processing_status[job_id]['completed'] = True
                logger.info(f"✅ การสร้างรายงานเสร็จสมบูรณ์! ไฟล์ ZIP: {zip_file_path.split(os.sep)[-1]}")
            else:
                with status_lock:
                    processing_status[job_id]['error'] = "ไม่พบโฟลเดอร์ชั่วคราวสำหรับสร้าง ZIP"
                    processing_status[job_id]['completed'] = True
                logger.error(f"❌ ไม่พบโฟลเดอร์ชั่วคราว '{temp_dir}' ไม่สามารถสร้าง ZIP ได้")
        else:
            with status_lock:
                 processing_status[job_id]['completed'] = True
                 processing_status[job_id]['error'] = "การประมวลผลถูกยกเลิก"

    except Exception as e:
        with status_lock:
            processing_status[job_id]['error'] = f"เกิดข้อผิดพลาดในระหว่างการประมวลผลเบื้องหลัง: {e}"
            processing_status[job_id]['completed'] = True
        logger.critical(f"❌ {processing_status[job_id]['error']}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                # ข้อความนี้จะถูกกรองโดย QueueHandler แล้ว
                logger.info(f"📁 ลบโฟลเดอร์ CSV/PDF ชั่วคราว: {temp_dir.split(os.sep)[-1]} แล้ว (ไม่รวมไฟล์ ZIP)") 
            except Exception as e:
                logger.error(f"❌ ข้อผิดพลาดในการลบโฟลเดอร์ CSV/PDF ชั่วคราว: {e}")

@app.route('/')
def upload_form():
    """แสดงหน้าฟอร์มสำหรับอัปโหลดไฟล์ Excel"""
    return render_template('index.html')

@app.route('/generate_report', methods=['POST'])
def generate_report():
    """
    รับไฟล์ที่อัปโหลด แล้วเริ่มการประมวลผลในเบื้องหลัง
    """
    if 'excel_file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['excel_file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        job_id = str(uuid.uuid4())
        file_stream = io.BytesIO(file.read())
        
        with status_lock:
            processing_status[job_id] = {
                'total': -1, 
                'processed': 0, 
                'completed': False, 
                'error': None,
                'canceled': False,
                'results': [],
                'temp_dir': None,
                'zip_file_path': None,
                'timestamp': datetime.datetime.now()
            }
        logger.info(f"📂 ได้รับไฟล์ excel '{file.filename}' และเริ่มการประมวลผล (Job ID: {job_id})")

        thread = threading.Thread(target=process_file_in_background, args=(file_stream, job_id))
        thread.daemon = True
        thread.start()
        
        return jsonify({"message": "Processing started", "job_id": job_id})

@app.route('/status/<job_id>')
def get_status(job_id):
    """
    ตรวจสอบสถานะของงานที่กำลังประมวลผลอยู่
    """
    with status_lock:
        status = processing_status.get(job_id, {})
    return jsonify(status)

@app.route('/logs/<job_id>')
def get_logs(job_id):
    """
    ดึง log ของงานที่กำลังประมวลผลอยู่
    """
    logs = []
    while not log_queue.empty():
        try:
            logs.append(log_queue.get_nowait())
        except Exception:
            break
    return jsonify({"logs": logs})


@app.route('/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    """
    รับคำสั่งยกเลิกงานที่กำลังประมวลผลอยู่
    """
    with status_lock:
        if job_id in processing_status:
            processing_status[job_id]['canceled'] = True
            logger.info(f"⛔ ได้รับคำขอยกเลิกงาน (Job ID: {job_id})")
            return jsonify({"message": "Job cancellation requested"}), 200
        else:
            logger.warning(f"⚠️ พยายามยกเลิกงานที่ไม่พบ (Job ID: {job_id})")
            return jsonify({"error": "Job not found"}), 404

@app.route('/download_report/<job_id>')
def download_report(job_id):
    """
    ให้ผู้ใช้ดาวน์โหลดไฟล์ ZIP ที่สร้างขึ้น
    """
    with status_lock:
        job_info = processing_status.get(job_id)

    if not job_info:
        logger.error(f"❌ ไม่พบข้อมูลงานสำหรับดาวน์โหลด (Job ID: {job_id})")
        return jsonify({"error": "Job not found or not ready for download. It might be too old or cancelled."}), 404

    zip_file_path = job_info.get('zip_file_path')

    if not zip_file_path or not os.path.exists(zip_file_path):
        logger.error(f"❌ ไม่พบไฟล์ ZIP หรือยังสร้างไม่เสร็จ (Job ID: {job_id}). Path: {zip_file_path}")
        if job_info.get('completed') and not zip_file_path:
            return jsonify({"error": "Report completed with no ZIP file generated (internal error)"}), 500
        return jsonify({"error": "Report not yet generated or file not found"}), 404
    
    try:
        directory = tempfile.gettempdir()
        filename = os.path.basename(zip_file_path)
        logger.info(f"📥 กำลังส่งไฟล์ ZIP: {filename} จาก {directory} (Job ID: {job_id})")
        
        response = send_from_directory(
            directory=directory,
            path=filename,
            as_attachment=True,
            mimetype='application/zip',
            download_name="Customer Report by Hour.zip"
        )
        
        return response

    except Exception as e:
        logger.critical(f"❌ ข้อผิดพลาดร้ายแรงในการส่งไฟล์ ZIP: {e} (Job ID: {job_id})")
        return jsonify({"error": f"Failed to serve file: {e}"}), 500

def cleanup_old_jobs():
    """
    ลบสถานะงานและไฟล์ ZIP เก่าๆ ออกจากระบบ
    รันเป็น background process
    """
    logger.info("🧹 เริ่มต้นกระบวนการล้างข้อมูลงานเก่า...")
    current_time = datetime.datetime.now()
    jobs_to_remove = []

    retention_hours = 24
    retention_seconds = retention_hours * 3600

    with status_lock:
        for job_id, job_info in list(processing_status.items()):
            if job_info.get('completed') and job_info.get('timestamp'): 
                job_timestamp = job_info['timestamp']
                if (current_time - job_timestamp).total_seconds() > retention_seconds:
                    jobs_to_remove.append(job_id)
            elif (not job_info.get('completed')) and (current_time - job_info.get('timestamp', current_time)).total_seconds() > (retention_seconds / 4):
                logger.warning(f"⚠️ พบงานค้างเก่า (ไม่สมบูรณ์) กำลังถูกลบ: {job_id}")
                jobs_to_remove.append(job_id)


    for job_id in jobs_to_remove:
        with status_lock:
            job_info = processing_status.pop(job_id, None) 
        if job_info:
            zip_file_path = job_info.get('zip_file_path')
            if zip_file_path and os.path.exists(zip_file_path):
                try:
                    os.remove(zip_file_path)
                    logger.info(f"🗑️ ลบไฟล์ ZIP เก่า: {os.path.basename(zip_file_path)} (Job ID: {job_id})")
                except Exception as e:
                    logger.error(f"❌ ข้อผิดพลาดในการลบไฟล์ ZIP เก่า: {e} (Job ID: {job_id})")
            logger.info(f"✨ ล้างสถานะงานสำหรับ Job ID: {job_id} แล้ว")
    logger.info("🧹 กระบวนการล้างข้อมูลงานเก่าเสร็จสมบูรณ์")
    threading.Timer(retention_seconds / 2, cleanup_old_jobs).start()

if __name__ == '__main__':
    cleanup_thread = threading.Thread(target=cleanup_old_jobs)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    app.run(debug=True)