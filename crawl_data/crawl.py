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

class JobStreetSpider(scrapy.Spider):
    name = "jobstreet"
    # URL bắt đầu là trang 1
    start_urls = ["https://www.jobstreet.vn/j?sp=homepage"]
    skills = ['python', 'java', 'c#', 'c++', 'javascript', 'php', 'sql', 'nosql', 'mongodb', 'mysql', 'postgresql', 
              'sqlserver', 'oracle', 'database', 'etl', 'hadoop', 'spark', 'machine learning', 'deep learning', 'data science', 
              'data engineering', 'artificial intelligence', 'computer vision', 'natural language processing', 'Server system',
              'IT Infrastructure', 'IT Security', 'IT Support', 'IT Management', 'IT Consultant', 'IT Architect', 'IT Director', 'IT Manager', 
              'IT Engineer', 'IT Analyst', 'IT Developer', 'IT Tester', 'IT Designer', 'IT Support', 'IT Manager', 'IT Director', 
              'Network system', 'cloud', 'deploy',]
    levels = ['junior', 'middle', 'senior', 'expert', 'leader', 'manager', 'director']

    positions = ['developer', 'engineer', 'analyst', 'architect', 'designer', 'tester', 'support', 'manager', 'director']
    type = [{'Toàn thời gian': 'full-time'}, {'Bán thời gian': 'part-time'}, {'Tạm thời': 'temporary'}, {'Thực tập': 'internship'}]

    # Thêm custom headers với User-Agent
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

    def __init__(self, *args, **kwargs):
        super(JobStreetSpider, self).__init__(*args, **kwargs)
        self.driver = webdriver.Chrome()  # Hoặc driver khác tùy bạn dùng
        self.jobs_data = []  # Danh sách để lưu dữ liệu job
        self.current_page = 1
        self.max_pages = 50
        # Thêm delay giữa các request để tránh bị chặn
        self.download_delay = 1  # Delay 5 giây giữa các request
        # Thêm set để theo dõi các URL đã xử lý
        self.processed_job_urls = set()
        self.count_job = 0
        self.count_job_processed = 0
        # Thêm set để theo dõi các job đã lưu vào CSV
        self.saved_job_urls = set()
        # Thêm dictionary để theo dõi job theo title và company
        self.processed_job_titles = {}

    def parse(self, response):
        # Sử dụng Selenium để load trang
        self.driver.get(response.url)
        
        # Tăng thời gian chờ
        wait = WebDriverWait(self.driver, 10)
        
        try:
            # Tìm tất cả các thẻ a chứa URL công việc
            job_links_elements = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "h2.job-title a.job-link"))
            )
            
            # Lấy href từ các thẻ a
            job_links = []
            for elem in job_links_elements:
                url = elem.get_attribute('href')
                if url and url not in job_links:
                    job_links.append(url)
            
            self.logger.info(f"Trang {self.current_page}: Đã lấy được {len(job_links)} link job")
            
            for job_link in job_links:
                # Kiểm tra xem URL này đã được xử lý chưa
                if job_link not in self.processed_job_urls:
                    self.processed_job_urls.add(job_link)
                    self.count_job_processed += 1
                    yield scrapy.Request(url=job_link, callback=self.parse_job_detail, meta={'job_link': job_link})
            
        except Exception as e:
            self.logger.error(f"Error: {str(e)}")
            # Chụp screenshot để debug
            self.driver.save_screenshot(f"error_page_{self.current_page}.png")
            # Không gọi closed ở đây, để Scrapy tự xử lý
        
        # Hiển thị tiến độ đã crawl
        self.logger.info(f"Đang ở trang {self.current_page}/{self.max_pages}. Còn lại {self.max_pages - self.current_page} trang.")
        
        # Tiếp tục với phần code chuyển trang tiếp theo
        if self.current_page < self.max_pages:
            self.current_page += 1
            if self.current_page == 2:
                next_page_url = "https://www.jobstreet.vn/j?disallow=true&l=&p=2&q=&sp=homepage&surl=0&tk=aA7V3B7kMB6XGejXMyFe-U89tzbNXGkbBSJjWUwoX"
            else:
                next_page_url = f"https://www.jobstreet.vn/j?disallow=true&l=&p={self.current_page}&q=&sp=homepage&surl=0&tk=aA7V3B7kMB6XGejXMyFe-U89tzbNXGkbBSJjWUwoX"
            
            self.logger.info(f"Chuyển đến trang {self.current_page}: {next_page_url}")
            yield scrapy.Request(url=next_page_url, callback=self.parse)
        # Không cần else ở đây, để Scrapy tự xử lý khi không còn request nào

    def parse_job_detail(self, response):
        # Kiểm tra URL đã được xử lý chưa
        if "/company" in response.url or response.url in self.saved_job_urls:
            return
        
        # Lấy thông tin cơ bản của job
        job_title = response.css('h1.job-title.heading.-size-xxlarge.-weight-700::text').get()  
        company = response.css('a.company::text').get()
        
        # Tạo key duy nhất từ title và company để tránh trùng lặp
        job_key = f"{job_title}_{company}"
        
        # Kiểm tra xem job này đã được xử lý chưa
        if job_key in self.processed_job_titles:
            self.logger.info(f"Bỏ qua job trùng lặp: {job_title} - {company}")
            return
        
        # Đánh dấu job này đã được xử lý
        self.processed_job_titles[job_key] = True
        
        # Đánh dấu URL này đã được xử lý và lưu
        self.saved_job_urls.add(response.url)
        self.count_job += 1
        time.sleep(random.uniform(3, 7))
        
        # Đánh dấu URL này đã được xử lý
        self.processed_job_urls.add(response.url)
        
        job_location = response.css('a.location::text').get()
        time_left = datetime.now().strftime('%d/%m/%Y')
        first_seen = time_left
        last_processed_time = time_left
        
        # Log extracted basic info
        self.logger.info(f"Đang xử lý job: {job_title} - {company}")
        
        # Extract job description text
        job_description = ' '.join(response.css('#job-description-container ::text').getall()).lower()
        
        # Find skills in job description
        found_skills = []
        for skill in self.skills:
            if skill.lower() in job_description:
                found_skills.append(skill)
        
        # Find job level in job description
        found_levels = []
        for level in self.levels:
            if level.lower() in job_description:
                found_levels.append(level)
                
        # Find positions in job description
        found_positions = []
        for position in self.positions:
            if position.lower() in job_description:
                found_positions.append(position)
        
        # Khởi tạo job_skills và job_level trước khi sử dụng
        job_skills = ''
        job_level = ''
        
        # Update job_skills and job_level with found values
        if found_skills:
            job_skills = ', '.join(found_skills) if not job_skills else job_skills + ', ' + ', '.join(found_skills)
        
        if found_levels:
            job_level = ', '.join(found_levels) if not job_level else job_level + ', ' + ', '.join(found_levels)

        # Fix: Check if job_location is None before calling split
        search_city = job_location.split(', ')[len(job_location.split(', ')) - 1] if job_location else ''
        search_country = 'Việt Nam'
        search_position = ''
        
        # Update search_position with found positions if empty
        if not search_position and found_positions:
            search_position = ', '.join(found_positions)

        job_type = ''
        # Fix: SelectorList doesn't have items() method
        job_type_elements = response.css('#job-description-container::text').getall()
        if job_type_elements:
            job_type = ', '.join(job_type_elements)
        
        job_data = {
            'job_title': job_title,
            'job_location': job_location,
            'company': company,
            'search_city': search_city,
            'job_skills': job_skills,
            'last_processed_time': last_processed_time if last_processed_time else None,
            'first_seen': first_seen,
            'search_country': search_country,
            'search_position': search_position,
            'job_level': job_level,
            'job_type': job_type,
        }

        # Thêm job_data vào danh sách
        self.jobs_data.append(job_data)
            
        # Ghi dữ liệu vào file CSV ngay lập tức sau khi xử lý
        try:
            # Kiểm tra xem file đã tồn tại chưa để quyết định có ghi header không
            file_exists = os.path.isfile('jobs_data_jobstreets.csv')
            
            # Ghi dữ liệu vào file CSV
            with open('jobs_data_jobstreets.csv', 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=job_data.keys())
                # Chỉ ghi header nếu file chưa tồn tại
                if not file_exists:
                    writer.writeheader()
                writer.writerow(job_data)
            self.logger.info(f"Đã thêm job #{len(self.jobs_data)}: {job_title} - {company}")
        except Exception as e:
            self.logger.error(f"Lỗi khi ghi file CSV: {str(e)}")
        
        yield job_data

    def closed(self, reason):
        # Không cần xóa file CSV cũ vì chúng ta đã ghi dữ liệu ngay khi xử lý
        # Cũng không cần ghi lại toàn bộ dữ liệu vì đã ghi từng mục một
        
        self.logger.info(f"Spider đã hoàn thành. Tổng số job đã xử lý: {self.count_job}")
        self.logger.info(f"Tổng số job đã lưu vào CSV: {len(self.jobs_data)}")
        self.logger.info(f"File CSV được lưu tại: {os.path.abspath('jobs_data_jobstreets.csv')}")
        self.driver.quit()

if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    # Thêm cấu hình để xử lý lỗi 429
    settings = get_project_settings()
    settings.update({
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'RETRY_TIMES': 10,
        'RETRY_DELAY': 10,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,  # Giảm số lượng request đồng thời
    })
    process = CrawlerProcess(settings)
    process.crawl(JobStreetSpider)
    process.start()
    
       
