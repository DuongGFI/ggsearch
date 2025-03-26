import asyncio
import csv
import io
import json
import logging
from datetime import datetime

import gspread
import requests
import google.generativeai as genai
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

# Cấu hình hệ thống
GOOGLE_SHEETS_SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
# Cấu hình từ file cấu hình (AI_config) của bạn
# Ví dụ:
import os
CUSTOM_SEARCH_API_KEY = os.getenv("GGSEARCH_API_KEY")
SEARCH_ENGINE_ID = os.getenv("SEARCH_ENGINE_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MAX_CONCURRENT_REQUESTS = 10

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

app = FastAPI(title="Webservice tìm kiếm & phân tích")

#############################################
# Các hàm hỗ trợ (sử dụng lại từ đoạn code gốc)
#############################################

async def crawl_webpage(url: str) -> str:
    """Crawl webpage không đồng bộ với logging chi tiết"""
    async with semaphore:
        try:
            # Sử dụng requests trong async không phải là tối ưu nhưng đây chỉ là ví dụ
            # Bạn có thể thay bằng thư viện aiohttp hoặc AsyncWebCrawler của bạn nếu có.
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                content = response.text
                logging.info(f"🕷️ Crawled successfully: {url} ({len(content)} chars)")
                # Giới hạn 20000 ký tự như trong code gốc
                return content[:20000]
            else:
                logging.error(f"🚨 Crawl failed (HTTP {response.status_code}): {url}")
                return ""
        except Exception as e:
            logging.error(f"🚨 Crawl failed: {url} - {str(e)[:200]}")
            return ""

def init_services():
    """Khởi tạo dịch vụ với logging xác thực"""
    try:
        creds = Credentials.from_service_account_file(
            'service-account.json', 
            scopes=GOOGLE_SHEETS_SCOPE
        )
        gc = gspread.authorize(creds)
        logging.info("🔑 Google Sheets authentication successful")
        
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash-lite')
        logging.info("🧠 Gemini API initialized")
        
        return gc, model
    except Exception as e:
        logging.critical(f"🔴 Critical initialization error: {str(e)}")
        raise

def get_keywords(gc, sheet_url):
    """Lấy dữ liệu keywords (cột A), timerange (cột B) và num (cột C) từ Google Sheets với logging chi tiết"""
    try:
        spreadsheet = gc.open_by_url(sheet_url)
        worksheet = spreadsheet.sheet1  # sử dụng sheet đầu tiên
        data = worksheet.get_all_values()
        
        if not data or len(data) < 2:
            logging.warning("Không có dữ liệu ngoài tiêu đề trên sheet")
            return []
        
        entries = []
        # Giả sử hàng đầu tiên là tiêu đề nên dữ liệu bắt đầu từ hàng thứ 2
        for idx, row in enumerate(data[1:], start=2):
            if len(row) < 3:
                logging.warning(f"Hàng {idx} không đủ dữ liệu: {row}")
                continue
            
            keyword = row[0].strip()
            timerange = row[1].strip()  # ví dụ: 'd1', 'w1', 'm1'
            try:
                num = int(row[2].strip()) if row[2].strip() != "" else 3
            except ValueError:
                logging.warning(f"Giá trị không hợp lệ tại hàng {idx} ở cột num: {row[2]}")
                num = 3

            entries.append({
                "keyword": keyword,
                "timerange": timerange,
                "num": num
            })
        
        logging.info(f"📖 Đã tìm thấy {len(entries)} bản ghi trên sheet")
        return entries

    except WorksheetNotFound:
        logging.error("📄 Worksheet không tồn tại - kiểm tra tên sheet/tab")
        return []
    except APIError as e:
        logging.error(f"🔐 Sheets API Error: {e.response.json()['error']['message']}")
        return []

def google_search(keyword, timerange=None, num=3):
    """Tìm kiếm Google với logging request/response"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': CUSTOM_SEARCH_API_KEY, 
        'cx': SEARCH_ENGINE_ID, 
        'q': keyword, 
        'num': num
    }
    if timerange:
        params['dateRestrict'] = timerange
    try:
        logging.debug(f"🔎 Searching for: {keyword} với timerange={timerange} và num={num}")
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code != 200:
            logging.error(f"⚡ Search API Error {response.status_code}: {response.text[:200]}")
            return []
            
        results = response.json().get('items', [])
        logging.info(f"🌐 Found {len(results)} results for: {keyword}")
        return [item['link'] for item in results]
        
    except Exception as e:
        logging.error(f"🌐 Search failed for {keyword}: {str(e)[:200]}")
        return []
    
def extract_json_from_response(gemini_response):
    start_index = gemini_response.find('{')
    end_index = gemini_response.rfind('}') + 1
    if start_index != -1 and end_index != -1:
        json_text = gemini_response[start_index:end_index]
        try:
            response_data = json.loads(json_text)
            return response_data
        except json.JSONDecodeError:
            return "Đã xảy ra lỗi khi phân tích phản hồi từ Gemini."
    else:
        return "Không tìm thấy dữ liệu JSON trong phản hồi."
        
def analyze_content(content, model):
    """Phân tích nội dung với logging đầy đủ"""
    prompt = f"""[YÊU CẦU BẮT BUỘC] 
    1. Trả về KẾT QUẢ DUY NHẤT dưới dạng JSON hợp lệ
    2. Không thêm bất kỳ giải thích hay markdown nào
    3. Đảm bảo đúng cấu trúc

    PHÂN TÍCH TIN TỨC:
    {content}

    KẾT QUẢ PHẢI THEO MẪU:
    {{
        "sentiment": "positive|neutral|negative",
        "impact": "high|medium|low",
        "reason": "lý do ngắn gọn dưới 20 từ"
    }}"""
    try:
        response = model.generate_content(prompt)
        analysis = extract_json_from_response(response.text)
        
        if not all(key in analysis for key in ['sentiment','impact','reason']):
            raise ValueError("Invalid analysis format")
            
        logging.debug(f"📊 Analysis result: {json.dumps(analysis, indent=2)}")
        return analysis
        
    except json.JSONDecodeError:
        logging.error("🔠 Failed to parse Gemini response as JSON")
        raise
    except ValueError as ve:
        logging.error(f"📉 Invalid analysis format: {str(ve)}")
        raise
    except Exception as e:
        logging.error(f"🧠 Analysis failed: {str(e)[:200]}")
        raise

def save_results(gc, sheet_url, data):
    """Lưu kết quả với logging chi tiết vào Google Sheets"""
    if not data:
        logging.warning("📭 No data to save")
        return

    try:
        spreadsheet = gc.open_by_url(sheet_url)
        try:
            sheet = spreadsheet.worksheet('Results')
            logging.info("📊 Found existing Results worksheet")
        except WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title='Results', rows=1000, cols=20)
            sheet.append_row(['Keyword', 'URL', 'Đánh giá', 'Tác động', 'Lý do', 'Ngày phân tích'])
            logging.info("📄 Created new Results worksheet")
        
        logging.debug(f"💾 Saving {len(data)} rows to sheet")
        sheet.append_rows(data)
        logging.info(f"✅ Successfully saved {len(data)} records")
        
    except APIError as e:
        logging.error(f"💾 Save failed - API Error: {e.response.json()['error']['message']}")
    except Exception as e:
        logging.error(f"💾 Save failed: {str(e)[:200]}")

async def process_keyword(gc, model, entry, output_sheet_url):
    """Xử lý từng entry chứa keyword, timerange và num với logging chi tiết"""
    keyword = entry.get("keyword")
    timerange = entry.get("timerange")
    num = entry.get("num", 3)
    
    logging.info(f"🔨 Starting processing for: {keyword} (timerange={timerange}, num={num})")
    results = []
    
    try:
        urls = google_search(keyword, timerange, num)
        if not urls:
            logging.warning(f"🌐 No URLs found for: {keyword}")
            return []
            
        logging.info(f"🌍 Crawling {len(urls)} URLs for: {keyword}")
        # Sử dụng asyncio.gather để crawl các URL
        contents = await asyncio.gather(*[crawl_webpage(url) for url in urls])
        
        success_count = 0
        for url, content in zip(urls, contents):
            if not content:
                logging.warning(f"⚠️ Empty content for: {url}")
                continue
                
            try:
                analysis = analyze_content(content, model)
                results.append([
                    keyword,
                    url,
                    analysis.get('sentiment', 'N/A'),
                    analysis.get('impact', 'N/A'),
                    analysis.get('reason', 'N/A'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ])
                success_count += 1
            except Exception:
                logging.error(f"🔧 Failed to analyze: {url}")
                
        logging.info(f"📈 Processed {success_count}/{len(urls)} URLs successfully for: {keyword}")
        return results
        
    except Exception as e:
        logging.error(f"🔥 Failed to process keyword {keyword}: {str(e)[:200]}")
        return []

#############################################
# ENDPOINT 1: Xử lý dựa trên Google Sheets
#############################################

@app.get("/process_sheet")
async def process_sheet(sheet_url: str):
    """
    Endpoint nhận vào URL của Google Sheets chứa dữ liệu đầu vào (keyword, timerange, num)
    và cũng là nơi lưu kết quả đầu ra.
    """
    try:
        gc, model = init_services()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Initialization error: " + str(e))
        
    entries = get_keywords(gc, sheet_url)
    if not entries:
        raise HTTPException(status_code=400, detail="Không tìm thấy dữ liệu hợp lệ trên sheet.")
    
    logging.info(f"🚀 Starting processing for {len(entries)} keywords from sheet")
    tasks = [process_keyword(gc, model, entry, sheet_url) for entry in entries]
    all_results = await asyncio.gather(*tasks)
    total_records = sum(len(batch) for batch in all_results if batch)
    
    # Lưu kết quả vào Google Sheets
    for result_batch in all_results:
        if result_batch:
            save_results(gc, sheet_url, result_batch)
    
    return JSONResponse(content={
        "message": "Processing completed",
        "total_keywords": len(entries),
        "total_records": total_records
    })

#############################################
# ENDPOINT 2: Xử lý theo JSON đầu vào và trả về CSV
#############################################

from pydantic import BaseModel

class SearchCriteria(BaseModel):
    keyword: str
    timerange: str = None  # ví dụ: 'd1', 'w1', 'm1'
    num: int = 3

@app.post("/search")
async def search_and_download(criteria: SearchCriteria):
    """
    Endpoint nhận JSON chứa keyword, timerange và num.
    Thực hiện tìm kiếm, crawl, phân tích và trả về kết quả ở dạng file CSV.
    """
    # Khởi tạo Gemini model (không cần Google Sheets cho endpoint này)
    try:
        # Ta chỉ cần model cho phân tích nội dung
        _, model = init_services()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Initialization error: " + str(e))
    
    keyword = criteria.keyword
    timerange = criteria.timerange
    num = criteria.num
    
    logging.info(f"🔨 Processing search for: {keyword} (timerange={timerange}, num={num})")
    
    try:
        urls = google_search(keyword, timerange, num)
        if not urls:
            raise HTTPException(status_code=404, detail="Không tìm thấy kết quả tìm kiếm")
        
        contents = await asyncio.gather(*[crawl_webpage(url) for url in urls])
        results = []
        for url, content in zip(urls, contents):
            if not content:
                continue
            try:
                analysis = analyze_content(content, model)
                results.append([
                    keyword,
                    url,
                    analysis.get('sentiment', 'N/A'),
                    analysis.get('impact', 'N/A'),
                    analysis.get('reason', 'N/A'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ])
            except Exception:
                continue
        
        if not results:
            raise HTTPException(status_code=500, detail="Không thể phân tích nội dung được")
        
        # Tạo file CSV từ kết quả
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Keyword', 'URL', 'Đánh giá', 'Tác động', 'Lý do', 'Ngày phân tích'])
        writer.writerows(results)
        output.seek(0)
        
        filename = f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
        
        return StreamingResponse(output, media_type="text/csv", headers=headers)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail="Processing error: " + str(e))
