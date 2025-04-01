import csv
from datetime import datetime, timedelta
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import os
# Thêm thư viện cho 2captcha
import requests
import logging
from logging.handlers import RotatingFileHandler
import json
import uuid
import traceback

class JobStreetSpider(scrapy.Spider):
    name = "jobstreet"
    start_urls = ["https://careerviet.vn/viec-lam/tat-ca-viec-lam-vi.html"]
    skills = ['python', 'java', 'c#', 'c++', 'javascript', 'php', 'sql', 'nosql', 'mongodb', 'mysql', 'postgresql', 
              'sqlserver', 'oracle', 'database', 'etl', 'hadoop', 'spark', 'machine learning', 'deep learning', 'data science', 
              'data engineering', 'artificial intelligence', 'computer vision', 'natural language processing', 'Server system',
              'IT Infrastructure', 'IT Security', 'IT Support', 'IT Management', 'IT Consultant', 'IT Architect', 'IT Director', 'IT Manager', 
              'IT Engineer', 'IT Analyst', 'IT Developer', 'IT Tester', 'IT Designer', 'IT Support', 'IT Manager', 'IT Director', 
              'Network system', 'cloud', 'deploy', 'Giọng nói rõ ràng, dễ nghe', 'Tốt nghiệp đại học', 'Tốt nghiệp cao đẳng', 'Tốt nghiệp THPT',
              'Vui vẻ, nhiệt tình, thân thiện', ]
    levels = ['junior', 'middle', 'senior', 'expert', 'leader', 'manager', 'director']

    positions = ['developer', 'engineer', 'analyst', 'architect', 'designer', 'tester', 'support', 'manager', 'director']

    # Thêm custom headers với User-Agent
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

    def __init__(self, *args, **kwargs):
        super(JobStreetSpider, self).__init__(*args, **kwargs)
        # Thêm options cho Chrome để giảm khả năng bị phát hiện
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.jobs_data = [] 
        self.current_page = 1
        # self.max_pages = 510
        self.max_pages = 510
        self.processed_job_urls = set()
        self.count_job = 0
        self.count_job_processed = 0
        self.saved_job_urls = set()
        self.processed_job_titles = {}
        
        # Thiết lập hệ thống ghi log lỗi
        self.setup_error_logging()
        
        # Dictionary để lưu trữ các lỗi
        self.errors = {
            'parsing_errors': [],
            'saving_errors': [],
            'network_errors': [],
            'selector_errors': []
        }
        
        # Tạo thư mục để lưu các file CSV theo trang
        self.csv_dir = 'csv_by_page'
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        
        # Biến để theo dõi jobs của trang hiện tại
        self.current_page_jobs = []
        self.current_page_job_count = 0
        self.current_page_processed_count = 0

        # Thêm biến để lưu trữ job theo trang
        self.jobs_by_page = {}

    def setup_error_logging(self):
        """Thiết lập hệ thống ghi log lỗi vào file"""
        # Tạo thư mục logs nếu chưa tồn tại
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Thiết lập logger cho file
        self.error_logger = logging.getLogger('error_logger')
        self.error_logger.setLevel(logging.ERROR)
        
        # Tạo handler cho file log với rotation
        handler = RotatingFileHandler(
            'logs/careerviet_errors.log', 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.error_logger.addHandler(handler)
        
        # Tạo file JSON để lưu lỗi có cấu trúc
        self.error_json_file = 'logs/careerviet_errors.json'
        if not os.path.exists(self.error_json_file):
            with open(self.error_json_file, 'w', encoding='utf-8') as f:
                json.dump([], f)

    def log_error(self, error_type, message, details=None, url=None):
        """Ghi lỗi vào log file và JSON"""
        # Tạo ID duy nhất cho lỗi
        error_id = str(uuid.uuid4())
        
        # Tạo thông tin lỗi
        error_info = {
            'id': error_id,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': error_type,
            'message': message,
            'url': url,
            'details': details
        }
        
        # Ghi vào log file
        log_message = f"Error ID: {error_id} - Type: {error_type} - URL: {url} - Message: {message}"
        self.error_logger.error(log_message)
        
        # Thêm vào dictionary lỗi
        if error_type in self.errors:
            self.errors[error_type].append(error_info)
        else:
            self.errors[error_type] = [error_info]
        
        # Ghi vào file JSON
        try:
            # Đọc dữ liệu hiện tại
            with open(self.error_json_file, 'r', encoding='utf-8') as f:
                try:
                    current_errors = json.load(f)
                except json.JSONDecodeError:
                    current_errors = []
            
            # Thêm lỗi mới
            current_errors.append(error_info)
            
            # Ghi lại vào file
            with open(self.error_json_file, 'w', encoding='utf-8') as f:
                json.dump(current_errors, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Không thể ghi lỗi vào file JSON: {str(e)}")
    
    def extract_job_links(self, url):
        """Hàm riêng để lấy tất cả các link job từ một trang"""
        job_links = []
        try:
            self.driver.get(url)
            time.sleep(random.uniform(1, 2))  # Thêm delay ngẫu nhiên để tránh bị chặn
            
            # Thử nhiều selector khác nhau để tìm job links
            job_links_elements = self.driver.find_elements(By.CSS_SELECTOR, ".job_link")
            
            # Nếu vẫn không tìm thấy, thử với XPath
            if not job_links_elements:
                job_links_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'job_link')]")
            
            # Lấy href từ các thẻ a
            for elem in job_links_elements:
                url = elem.get_attribute('href')
                if url and url not in self.processed_job_urls:
                    job_links.append(url)
                    self.processed_job_urls.add(url)
            
            self.logger.info(f"Trang {self.current_page}: Đã lấy được {len(job_links)} link job")
            
            if not job_links:
                self.driver.save_screenshot(f"no_jobs_page_{self.current_page}.png")
                self.logger.error(f"Không tìm thấy job links trên trang {self.current_page}")
            
            return job_links
            
        except Exception as e:
            self.logger.error(f"Lỗi khi lấy job links từ trang {self.current_page}: {str(e)}")
            self.log_error(
                'parsing_errors',
                f"Lỗi khi lấy job links: {str(e)}",
                details=traceback.format_exc(),
                url=url
            )
            self.driver.save_screenshot(f"error_extract_links_page_{self.current_page}.png")
            return []

    def parse(self, response):
        """Hàm chính để thu thập tất cả các link job từ tất cả các trang"""
        self.logger.info(f"Bắt đầu thu thập tất cả các link job từ tất cả các trang")
        
        # Khởi tạo danh sách để lưu tất cả các link job theo trang
        all_job_links_by_page = {}
        
        # Thu thập link job từ trang đầu tiên
        job_links = self.extract_job_links(response.url)
        all_job_links_by_page[1] = job_links
        
        # Thu thập link job từ các trang tiếp theo
        while self.current_page < self.max_pages:
            self.current_page += 1
            next_page_url = f"https://careerviet.vn/viec-lam/tat-ca-viec-lam-trang-{self.current_page}-vi.html"
            if self.current_page == 2:
                next_page_url = "https://careerviet.vn/viec-lam/tat-ca-viec-lam-trang-2-vi.html"
            
            self.logger.info(f"Chuyển đến trang {self.current_page}: {next_page_url}")
            
            job_links = self.extract_job_links(next_page_url)
            all_job_links_by_page[self.current_page] = job_links
            
            # Thêm delay ngẫu nhiên giữa các trang
            time.sleep(random.uniform(1, 3))
        
        # Lưu tất cả các link job vào file để có thể sử dụng lại nếu cần
        with open('all_job_links_by_page.json', 'w', encoding='utf-8') as f:
            # Chuyển đổi keys từ int sang str để có thể serialize thành JSON
            serializable_dict = {str(k): v for k, v in all_job_links_by_page.items()}
            json.dump(serializable_dict, f, ensure_ascii=False)
        
        # Tính tổng số link job đã thu thập
        total_links = sum(len(links) for links in all_job_links_by_page.values())
        self.logger.info(f"Đã thu thập tổng cộng {total_links} link job từ {self.current_page} trang")
        
        # Bắt đầu xử lý từng trang và các link job của trang đó
        for page_num, page_links in all_job_links_by_page.items():
            self.logger.info(f"Bắt đầu xử lý {len(page_links)} link job của trang {page_num}")
            
            # Khởi tạo danh sách để lưu job data của trang này
            page_key = f"page_{page_num}"
            if page_key not in self.jobs_by_page:
                self.jobs_by_page[page_key] = []
            
            # Xử lý từng link job của trang
            for index, job_link in enumerate(page_links):
                self.logger.info(f"Xử lý job link {index+1}/{len(page_links)} của trang {page_num}: {job_link}")
                yield scrapy.Request(
                    url=job_link,
                    callback=self.parse_job_detail,
                    meta={
                        'job_link': job_link, 
                        'page_num': page_num,
                        'dont_redirect': True, 
                        'handle_httpstatus_list': [302]
                    },
                    errback=self.handle_error,
                    dont_filter=True
                )
                # Thêm delay ngẫu nhiên giữa các request
                time.sleep(random.uniform(0.5, 1.5))

    def parse_job_detail(self, response):
        job_link = response.url
        # Lấy số trang từ metadata thay vì tính toán dựa trên số lượng job đã xử lý
        page_number = response.meta.get('page_num', 1)
        self.logger.info(f"Bắt đầu xử lý job link: {job_link} (Trang: {page_number})")
        
        try:
            # Tăng biến đếm số job đã xử lý
            self.count_job_processed += 1
            
            # Get job title
            job_title = response.css('h1.title::text').get() or response.css('.title h2::text').get()
            if job_title:
                job_title = job_title.strip()
                self.logger.info(f"Đã tìm thấy job title: {job_title}")
            else:
                # Thử thêm các selector khác
                job_title = response.css('h1::text').get() or response.xpath('//h1/text()').get()
                if job_title:
                    job_title = job_title.strip()
                    self.logger.info(f"Đã tìm thấy job title (selector thay thế): {job_title}")
                else:
                    # Log chi tiết HTML để debug
                    self.logger.error(f"Không tìm thấy tiêu đề job tại: {response.url}")
                    self.logger.error(f"HTML content: {response.text[:500]}...")  # Log một phần nội dung HTML
                    return
            
            company = response.css('a.employer.job-company-name::text').get() or ''
            if not company:
                # Thử thêm các selector khác
                company = response.css('.company-name::text').get() or response.css('.employer::text').get() or ''
            
            if company:
                company = company.strip()
                self.logger.info(f"Đã tìm thấy company: {company}")
            else:
                self.logger.warning(f"Không tìm thấy company cho job: {job_title} tại {response.url}")
            
            # Tạo job_key duy nhất hơn - MODIFIED to use just URL as the key
            job_key = job_link
            
            # Debug log để xem key đang được tạo ra
            self.logger.info(f"Job key: {job_key}")
            
            # Kiểm tra xem job này đã được xử lý chưa
            if job_key in self.processed_job_titles:
                self.logger.info(f"Bỏ qua job trùng lặp: {job_title} - {company}")
                return
            
            # Đánh dấu job này đã được xử lý
            self.processed_job_titles[job_key] = True
            
            # Đánh dấu URL này đã được xử lý và lưu
            self.saved_job_urls.add(response.url)
            self.count_job += 1
            self.current_page_job_count += 1

            # First try to get the detailed address if available
            detailed_location = response.css('.place-name span::text').get('').strip()
            # Then get the city/province
            city = response.css('.place-name .place::text').get('').strip()
            
            # Combine them, with city first (which is how the data appears in the CSV)
            if city and detailed_location:
                job_location = f"{city}, {detailed_location}"
            elif city:
                job_location = city
            elif detailed_location:
                job_location = detailed_location
            else:
                # Fallback to the original selector if the new structure isn't found
                job_location = response.css('#job-location span::text').get('').strip() + response.css('#job-location a::text').get('').strip() or ''
            
            # Add current date for time fields
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            time_left = current_time
            first_seen = current_time
            last_processed_time = current_time
            
            # Log extracted basic info
            self.logger.info(f"Đang xử lý job: {job_title} - {company}")
            
            # Extract job description text
            job_description = ' '.join(response.css('#section-job-skills .raw-content ::text').getall()).lower()
            
            # Extract job requirements
            job_requirements = []
            requirements_section = response.css('.detail-row.reset-bullet')
            for section in requirements_section:
                title = section.css('.detail-title::text').get('')
                if 'Yêu Cầu Công Việc' in title:
                    requirements = section.css('ul li::text').getall()
                    if requirements:
                        job_requirements = [req.strip() for req in requirements if req.strip()]
                    break
            
            # Convert requirements to string for easier searching
            job_requirements_text = ' '.join(job_requirements).lower()
            # Combine with job description for skill matching
            job_description = job_description + ' ' + job_requirements_text
            
            # Find skills in job description
            found_skills = []
            for skill in self.skills:
                if skill.lower() in job_description:
                    found_skills.append(skill)
            
            # Khởi tạo job_skills và job_level trước khi sử dụng
            job_skills = ''
            
            # Thay thế CSS selector phức tạp bằng cách sử dụng XPath hoặc CSS đơn giản hơn
            # Tìm tất cả các job-summary-item và lọc ra cái chứa "Cấp bậc"
            job_level = ''
            # Try to get job level from the detail-box structure
            job_level_element = response.css('.detail-box li:contains("Cấp bậc") p::text').get()
            if job_level_element:
                job_level = job_level_element.strip()
            else:
                # Fallback to the previous method if not found in detail-box
                for item in response.css('.job-summary-item'):
                    label = item.css('.summary-label::text').get('')
                    if 'Cấp bậc' in label:
                        job_level = item.css('.font-weight-bolder::text').get('').strip()
                        break
                    else:
                        job_level = ''
            
            # Update job_skills and job_level with found values
            if found_skills:
                job_skills = ', '.join(found_skills) if not job_skills else job_skills + ', ' + ', '.join(found_skills)

            # Fix: Check if job_location is None before calling split
            # Extract the last part of the location string which should be the city
            # For example, from "Phạm Hùng, Mai Dịch, Cầu Giấy, Hà Nội" we extract "Hà Nội"
            search_city = job_location.split(', ')[-1] if job_location else ''
            search_country = 'Việt Nam'
            search_position = ''
            
            # Clean job_branch data - extract text and join with commas
            # Extract job types from the detail-box structure
            job_branch_elements = response.css('.detail-box li:contains("Ngành nghề") p a::text').getall()
            if job_branch_elements:
                # Clean and join job types
                job_branch = [jt.strip() for jt in job_branch_elements if jt.strip()]
                # Convert to list format for CSV storage but remove unnecessary characters
                job_branch = str(job_branch).replace("'", "").replace("[", "").replace("]", "").replace("/", "").replace('"', "") if job_branch else ''
            else:
                # Fallback to the previous method if not found in detail-box
                job_branch_elements = response.css('.job-summary-item .font-weight-bolder a span::text').getall()
                job_branch = str([jt.strip() for jt in job_branch_elements if jt.strip()]).replace("'", "").replace("[", "").replace("]", "").replace("/", "").replace('"', "") if job_branch_elements else ''

            # Log trước khi tạo job_data
            self.logger.info(f"Chuẩn bị tạo job_data cho: {job_title} - {company}")
            
            job_data = {
                'job_title': job_title,
                'job_location': job_location,
                'company': company,
                'search_city': search_city,
                'job_skills': job_skills,
                'last_processed_time': last_processed_time,
                'first_seen': first_seen,
                'search_country': search_country,
                'search_position': search_position,
                'job_level': job_level,
                'job_branch': job_branch,
                'job_url': job_link,  # Add job URL to the data
            }

            # Log job_data để kiểm tra
            self.logger.info(f"Đã tạo job_data: {job_data}")
            
            # Thêm job_data vào danh sách trang tương ứng
            page_key = f"page_{page_number}"
            if page_key not in self.jobs_by_page:
                self.jobs_by_page[page_key] = []
            
            self.jobs_by_page[page_key].append(job_data)
            self.jobs_data.append(job_data)
            
            # Kiểm tra xem đã xử lý hết các job của trang này chưa
            # Lấy tổng số job của trang từ all_job_links_by_page
            try:
                with open('all_job_links_by_page.json', 'r', encoding='utf-8') as f:
                    all_links = json.load(f)
                    total_jobs_in_page = len(all_links.get(str(page_number), []))
                    
                    # Nếu đã xử lý đủ số job của trang hoặc đây là job cuối cùng, lưu vào CSV
                    if len(self.jobs_by_page[page_key]) >= total_jobs_in_page:
                        self.logger.info(f"Đã xử lý đủ {len(self.jobs_by_page[page_key])}/{total_jobs_in_page} job của trang {page_number}, lưu vào CSV")
                        self.save_page_to_csv(page_key)
            except Exception as e:
                self.logger.error(f"Lỗi khi kiểm tra số lượng job của trang {page_number}: {str(e)}")
                # Nếu có lỗi, cứ lưu sau khi đạt 20 job
                if len(self.jobs_by_page[page_key]) >= 20:
                    self.save_page_to_csv(page_key)
            
            yield job_data
            
        except Exception as e:
            error_message = f"Lỗi khi xử lý job {job_link}: {str(e)}"
            self.logger.error(error_message)
            # Ghi lỗi vào hệ thống log
            self.log_error(
                'parsing_errors',
                error_message,
                details=traceback.format_exc(),
                url=job_link
            )
            # Optionally take a screenshot for debugging
            try:
                self.driver.get(job_link)
                self.driver.save_screenshot(f"error_job_{job_link.split('/')[-1]}.png")
            except Exception as screenshot_error:
                self.logger.error(f"Lỗi khi chụp ảnh màn hình: {str(screenshot_error)}")
            
            # Important: increment the counter even for failed jobs
            self.current_page_job_count += 1
            
            # Check if we've processed all jobs on this page
            if self.current_page_job_count >= self.current_page_processed_count:
                self.logger.info(f"Đã xử lý xong tất cả {self.current_page_job_count} job trên trang {self.current_page}")
                # Lưu dữ liệu của trang hiện tại vào file CSV riêng
                self.save_current_page_to_csv()
                # Chuyển sang trang tiếp theo
                if self.current_page < self.max_pages:
                    yield self.go_to_next_page()

    def save_current_page_to_csv(self):
        """Lưu dữ liệu của trang hiện tại vào file CSV riêng"""
        if not self.current_page_jobs:
            self.logger.warning(f"Không có dữ liệu để lưu cho trang {self.current_page}")
            return
        
        try:
            # Tạo tên file CSV cho trang hiện tại
            csv_filename = os.path.join(self.csv_dir, f'page_{self.current_page}.csv')
            
            # Ghi dữ liệu vào file CSV
            with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.current_page_jobs[0].keys())
                writer.writeheader()
                writer.writerows(self.current_page_jobs)
            
            self.logger.info(f"Đã lưu {len(self.current_page_jobs)} job của trang {self.current_page} vào file {csv_filename}")
            
            # Reset danh sách jobs cho trang mới
            self.current_page_jobs = []
            self.current_page_job_count = 0
            self.current_page_processed_count = 0
        except Exception as e:
            error_message = f"Lỗi khi lưu file CSV cho trang {self.current_page}: {str(e)}"
            self.logger.error(error_message)
            self.log_error(
                'saving_errors',
                error_message,
                details=traceback.format_exc(),
                url=f"Page {self.current_page}"
            )

    def go_to_next_page(self):
        """Helper method to generate the next page request"""
        # Lưu dữ liệu của trang hiện tại trước khi chuyển sang trang mới
        self.save_current_page_to_csv()
        
        self.current_page += 1
        if self.current_page == 2:
            next_page_url = "https://careerviet.vn/viec-lam/tat-ca-viec-lam-trang-2-vi.html"
        else:
            next_page_url = f"https://careerviet.vn/viec-lam/tat-ca-viec-lam-trang-{self.current_page}-vi.html"
        
        self.logger.info(f"Chuyển đến trang {self.current_page}: {next_page_url}")
        return scrapy.Request(url=next_page_url, callback=self.parse)

    def handle_error(self, failure):
        # Log the error
        request = failure.request
        self.logger.error(f"Request failed: {request.url}, error: {repr(failure)}")
        self.log_error(
            'network_errors',
            f"Request failed: {repr(failure)}",
            details=failure.getTraceback(),
            url=request.url
        )

    def closed(self, reason):
        # Lưu các job còn lại trong bộ nhớ vào file CSV
        for page_key, jobs in self.jobs_by_page.items():
            if jobs:
                self.save_page_to_csv(page_key)
        
        # Thêm kiểm tra và lưu lại các job chưa được lưu
        self.logger.info(f"Spider đã hoàn thành với lý do: {reason}")
        self.logger.info(f"Tổng số job đã xử lý: {self.count_job_processed}")
        self.logger.info(f"Tổng số job đã lưu vào danh sách: {len(self.jobs_data)}")
        
        # Kiểm tra xem có job nào chưa được lưu không
        if self.count_job_processed > len(self.jobs_data):
            self.logger.warning(f"Có {self.count_job_processed - len(self.jobs_data)} job đã xử lý nhưng chưa được lưu vào danh sách!")
        
        # Tạo báo cáo tổng hợp lỗi
        self.generate_error_report()
        
        self.driver.quit()
    
    def generate_error_report(self):
        """Tạo báo cáo tổng hợp lỗi"""
        try:
            # Tạo báo cáo HTML
            report_path = 'logs/error_report.html'
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('<html><head><title>Careerviet Crawler Error Report</title>')
                f.write('<style>body{font-family:Arial;margin:20px}table{border-collapse:collapse;width:100%}')
                f.write('th,td{border:1px solid #ddd;padding:8px;text-align:left}')
                f.write('th{background-color:#f2f2f2}tr:nth-child(even){background-color:#f9f9f9}')
                f.write('.error-type{font-weight:bold;color:#d9534f}</style></head><body>')
                f.write(f'<h1>Careerviet Crawler Error Report</h1>')
                f.write(f'<p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>')
                
                # Thống kê tổng quan
                total_errors = sum(len(errors) for errors in self.errors.values())
                f.write(f'<h2>Overview</h2>')
                f.write(f'<p>Total errors: {total_errors}</p>')
                f.write('<ul>')
                for error_type, errors in self.errors.items():
                    if errors:
                        f.write(f'<li>{error_type}: {len(errors)}</li>')
                f.write('</ul>')
                
                # Chi tiết từng loại lỗi
                for error_type, errors in self.errors.items():
                    if errors:
                        f.write(f'<h2 class="error-type">{error_type} ({len(errors)})</h2>')
                        f.write('<table><tr><th>ID</th><th>Timestamp</th><th>URL</th><th>Message</th></tr>')
                        for error in errors:
                            f.write(f'<tr><td>{error["id"]}</td><td>{error["timestamp"]}</td>')
                            f.write(f'<td>{error["url"] or "N/A"}</td><td>{error["message"]}</td></tr>')
                        f.write('</table>')
                
                f.write('</body></html>')
            
            self.logger.info(f"Đã tạo báo cáo lỗi tại: {os.path.abspath(report_path)}")
        except Exception as e:
            self.logger.error(f"Lỗi khi tạo báo cáo lỗi: {str(e)}")

    def save_page_to_csv(self, page_key):
        """Lưu dữ liệu của một trang cụ thể vào file CSV"""
        if page_key not in self.jobs_by_page or not self.jobs_by_page[page_key]:
            self.logger.warning(f"Không có dữ liệu để lưu cho {page_key}")
            return
        
        try:
            # Tạo tên file CSV cho trang
            csv_filename = os.path.join(self.csv_dir, f'{page_key}.csv')
            
            # Ghi dữ liệu vào file CSV
            with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.jobs_by_page[page_key][0].keys())
                writer.writeheader()
                writer.writerows(self.jobs_by_page[page_key])
            
            self.logger.info(f"Đã lưu {len(self.jobs_by_page[page_key])} job của {page_key} vào file {csv_filename}")
            
            # Xóa dữ liệu đã lưu để giải phóng bộ nhớ
            self.jobs_by_page[page_key] = []
        except Exception as e:
            error_message = f"Lỗi khi lưu file CSV cho {page_key}: {str(e)}"
            self.logger.error(error_message)
            self.log_error(
                'saving_errors',
                error_message,
                details=traceback.format_exc(),
                url=f"{page_key}"
            )

if __name__ == '__main__':
    # Cải thiện xử lý ngoại lệ để tránh shutdown do lỗi
    try:
        # Thêm cấu hình để xử lý lỗi 429 và cải thiện hiệu suất
        settings = get_project_settings()
        settings.update({
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
            'RETRY_TIMES': 10,
            'RETRY_DELAY': 1,
            'DOWNLOAD_DELAY': 0.2,
            'RANDOMIZE_DOWNLOAD_DELAY': True,
            'CONCURRENT_REQUESTS': 16,
            'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
            'DOWNLOAD_TIMEOUT': 30,
            'JOBDIR': 'crawls/careerviet',
            'LOG_LEVEL': 'INFO',
            'COOKIES_ENABLED': False,
            # Thêm cấu hình để xử lý tín hiệu SIGINT (Ctrl+C)
            'EXTENSIONS': {
                'scrapy.extensions.telnet.TelnetConsole': None,
            },
            # Tắt các extension không cần thiết
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
                'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
            },
            # Tắt các middleware không cần thiết
            'SPIDER_MIDDLEWARES': {
                'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
                'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None,
                'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
                'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': None,
                'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
            },
            # Thêm cấu hình để sử dụng memory queue thay vì disk queue
            'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleLifoDiskQueue',
            'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.LifoMemoryQueue',
            'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.ScrapyPriorityQueue',
        })
              
        # Sử dụng CrawlerProcess với settings đã cấu hình
        process = CrawlerProcess(settings)
        process.crawl(JobStreetSpider)
        process.start()  # Bắt đầu crawl
        
    except KeyboardInterrupt:
        print("Crawler stopped manually by user")
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
    
       
