import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import json
import os

class TimViecCrawler:
    def __init__(self):
        self.base_url = "https://timviec.com.vn"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.jobs = []
        self.skills = ['python', 'java', 'c#', 'c++', 'javascript', 'php', 'sql', 'nosql', 'mongodb', 'mysql', 'postgresql', 
              'sqlserver', 'oracle', 'database', 'etl', 'hadoop', 'spark', 'machine learning', 'deep learning', 'data science', 
              'data engineering', 'artificial intelligence', 'computer vision', 'natural language processing', 'Server system',
              'IT Infrastructure', 'IT Security', 'IT Support', 'IT Management', 'IT Consultant', 'IT Architect', 'IT Director', 'IT Manager', 
              'IT Engineer', 'IT Analyst', 'IT Developer', 'IT Tester', 'IT Designer', 'IT Support', 'IT Manager', 'IT Director', 
              'Network system', 'cloud', 'deploy', 'Giọng nói rõ ràng, dễ nghe', 'Tốt nghiệp đại học', 'Tốt nghiệp cao đẳng', 'Tốt nghiệp THPT',
              'Vui vẻ, nhiệt tình, thân thiện', ]
        self.levels = ['junior', 'middle', 'senior', 'expert', 'leader', 'manager', 'director']

        self.positions = ['developer', 'engineer', 'analyst', 'architect', 'designer', 'tester', 'support', 'manager', 'director']
    
    def get_soup(self, url):
        """Lấy nội dung HTML từ URL và chuyển thành đối tượng BeautifulSoup"""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Lỗi khi truy cập {url}: {e}")
            return None
    
    def extract_job_info(self, job_element):
        """Trích xuất thông tin từ một phần tử việc làm"""
        try:
            title_element = job_element.select_one('.field-job-title a')
            title = title_element.text.strip() if title_element else "N/A"
            job_url = self.base_url + title_element['href'] if title_element and 'href' in title_element.attrs else "N/A"
            
            company_element = job_element.select_one('.field-job-company a')
            company = company_element.text.strip() if company_element else "N/A"
            company_url = self.base_url + company_element['href'] if company_element and 'href' in company_element.attrs else "N/A"
            
            location_element = job_element.select_one('.place .color-main')
            location = location_element.text.strip() if location_element else "N/A"

            job_details = self.crawl_job_details(job_url) if job_url != "N/A" else {}
            
            job_info = {
                'job_title': title,
                'job_url': job_url,
                'company': company,
                'company_url': company_url,
                'job_location': location,
                'search_city': location,
                'job_skills': job_details.get('skills', ''),
                'first_seen': time.strftime("%d/%m/%Y"),
                'search_country': 'Việt Nam',
                'search_position': title,
                'job_level': job_details.get('job_level', ''),
                'job_type': job_details.get('job_type', '')
            }
            
            return job_info
        except Exception as e:
            print(f"Lỗi khi trích xuất thông tin việc làm: {e}")
            return None
    
    def crawl_job_details(self, job_url):
        """Trích xuất thông tin chi tiết từ trang chi tiết việc làm"""
        try:
            print(f"Đang crawl chi tiết việc làm từ: {job_url}")
            soup = self.get_soup(job_url)
            if not soup:
                return {}
            
            details = {
                'skills': '',
                'job_level': '',
                'job_type': ''
            }
            
            job_description = soup.select_one('.job-description')
            if job_description:
                skills_section = job_description.find(lambda tag: tag.name == 'h3' and 'yêu cầu' in tag.text.lower())
                if skills_section:
                    skills_list = skills_section.find_next('ul')
                    if skills_list:
                        skills = [li.text.strip() for li in skills_list.find_all('li')]
                        details['skills'] = ', '.join(skills)
            
            job_level_element = soup.select_one('.job-level')
            if job_level_element:
                details['job_level'] = job_level_element.text.strip()
            
            job_type_element = soup.select_one('.job-type')
            if job_type_element:
                details['job_type'] = job_type_element.text.strip()
            
            return details
        except Exception as e:
            print(f"Lỗi khi crawl chi tiết việc làm: {e}")
            return {}
    
    def crawl_jobs_from_page(self, page_url):
        """Crawl tất cả việc làm từ một trang"""
        print(f"Đang crawl việc làm từ trang: {page_url}")
        soup = self.get_soup(page_url)
        if not soup:
            return []
        
        jobs_data = []
        job_elements = soup.select('.item-job-box')
        
        for job_element in job_elements:
            job_info = self.extract_job_info(job_element)
            if job_info:
                jobs_data.append(job_info)
                print(f"Đã crawl thông tin việc làm: {job_info['job_title']}")
                
                delay = random.uniform(1, 3)
                print(f"Chờ {delay:.2f} giây trước khi crawl việc làm tiếp theo...")
                time.sleep(delay)
        
        return jobs_data
    
    def crawl_multiple_pages(self, num_pages=5):
        """Crawl nhiều trang việc làm"""
        for page in range(1, num_pages + 1):
            page_url = f"{self.base_url}/tim-viec-lam?page={page}"
            print(f"Đang crawl trang {page}: {page_url}")
            
            page_jobs = self.crawl_jobs_from_page(page_url)
            self.jobs.extend(page_jobs)
            
            if page < num_pages:
                delay = random.uniform(3, 5)
                print(f"Chờ {delay:.2f} giây trước khi crawl trang tiếp theo...")
                time.sleep(delay)
        
        return self.jobs
    
    def save_to_csv(self, filename="jobs_data_timviec.csv"):
        """Lưu dữ liệu việc làm vào file CSV"""
        if not self.jobs:
            print("Không có dữ liệu việc làm để lưu.")
            return
        
        columns = [
            'job_title', 'job_location', 'company', 'search_city', 
            'job_skills', 'last_processed_time', 'first_seen', 
            'search_country', 'search_position', 'job_level', 'job_type'
        ]
        
        df = pd.DataFrame(self.jobs)
        
        for col in columns:
            if col not in df.columns:
                df[col] = ''
        
        df = df[columns]
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Đã lưu {len(self.jobs)} việc làm vào file {filename}")
    
    def save_to_excel(self, filename="timviec_jobs.xlsx"):
        """Lưu dữ liệu việc làm vào file Excel"""
        if not self.jobs:
            print("Không có dữ liệu việc làm để lưu.")
            return
        
        df = pd.DataFrame(self.jobs)
        df.to_excel(filename, index=False)
        print(f"Đã lưu {len(self.jobs)} việc làm vào file {filename}")

    def crawl_job_details_from_url(self, url):
        """Crawl thông tin chi tiết từ URL việc làm"""
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            job_title = soup.select_one('meta[property="og:title"]')['content'].split('(')[0].strip()
            job_title = str(job_title).replace("'", "").replace("[", "").replace("]", "").replace("/", "").replace('"', "") if job_title else ''
            # Extract search city from the location information
            search_city = ""
            location_element = soup.select_one('li:contains("Khu vực tuyển dụng:")')
            if location_element:
                city_link = location_element.select_one('a.color-main')
                if city_link:
                    search_city = city_link.text.strip()
            
            company_name = soup.select_one('meta[property="og:title"]')['content'].split('việc làm tại ')[1].strip() if 'việc làm tại' in soup.select_one('meta[property="og:title"]')['content'] else ""
            
            location = soup.select_one('meta[name="description"]')['content'].split('tại ')[1].split(',')[0].strip() if 'tại ' in soup.select_one('meta[name="description"]')['content'] else ""

            job_description = ""
            job_description_element = soup.select_one('script[type="application/ld+json"]')
            if job_description_element:
                try:
                    job_data = json.loads(job_description_element.string)
                    job_description = job_data.get('description', '')
                except:
                    pass

            job_type = ""
            if job_description_element:
                try:
                    job_data = json.loads(job_description_element.string)
                    job_type = job_data.get('employmentType', '')
                    if job_type == "FULL_TIME":
                        job_type = "Toàn thời gian"
                except:
                    pass
            
            job_data = {
                'job_title': job_title,
                'job_location': location,
                'company': company_name,
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
            
            return job_data
        except Exception as e:
            print(f"Lỗi khi crawl chi tiết việc làm từ {url}: {str(e)}")
            return None

    def extract_job_urls_from_html(self, html_content):
        """Trích xuất tất cả URL việc làm từ nội dung HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        job_urls = []
        
        job_elements = soup.select('.item-job-box')
        
        for job_element in job_elements:
            title_element = job_element.select_one('.field-job-title a')
            if title_element and 'href' in title_element.attrs:
                job_url = title_element['href']
                if not job_url.startswith('http'):
                    job_url = self.base_url + job_url
                job_urls.append(job_url)
        
        print(f"Đã tìm thấy {len(job_urls)} URL việc làm.")
        return job_urls

    def crawl_and_save_job_details(self, job_url, output_folder="job_details"):
        """Crawl thông tin chi tiết từ URL việc làm và lưu vào file riêng"""
        try:
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            job_data = self.crawl_job_details_from_url(job_url)
            
            if job_data:
                return job_data
            else:
                print(f"Không thể crawl thông tin chi tiết từ {job_url}")
                return None
        except Exception as e:
            print(f"Lỗi khi crawl thông tin chi tiết từ {job_url}: {str(e)}")
            return None

    def crawl_multiple_urls(self, urls, output_folder="job_details", delay_range=(1, 3), page_number=1):
        """Crawl nhiều URL và lưu tất cả vào một file CSV"""
        all_jobs = []
        
        for i, url in enumerate(urls):
            print(f"Đang crawl URL {i+1}/{len(urls)}: {url}")
            job_data = self.crawl_and_save_job_details(url, output_folder)
            
            if job_data:
                all_jobs.append(job_data)
            
            if i < len(urls) - 1:  
                delay_time = random.uniform(delay_range[0], delay_range[1])
                print(f"Đợi {delay_time:.2f} giây trước khi crawl URL tiếp theo...")
                time.sleep(delay_time)
        
        if all_jobs:
            csv_file_path = os.path.join(output_folder, f"jobs_page_{page_number}.csv")
            df = pd.DataFrame(all_jobs)
            df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
            print(f"Đã lưu {len(all_jobs)} việc làm vào file CSV: {csv_file_path}")
        
        print(f"Đã hoàn thành crawl {len(all_jobs)} việc làm thành công.")
        return all_jobs

    def crawl_from_html_content(self, html_content, output_folder="job_details"):
        """Crawl tất cả việc làm từ nội dung HTML và lưu vào file CSV"""
        job_urls = self.extract_job_urls_from_html(html_content)
        
        return self.crawl_multiple_urls(job_urls, output_folder)
    
    def crawl_direct_from_web(self, num_pages=5, output_folder="job_details"):
        """Crawl trực tiếp từ trang web và lưu mỗi trang vào một file CSV riêng"""
        all_jobs = []
        
        for page in range(1, num_pages + 1):
            page_url = f"{self.base_url}/tim-viec-lam?page={page}"
            print(f"Đang crawl trang {page}/{num_pages}: {page_url}")
            
            soup = self.get_soup(page_url)
            if not soup:
                print(f"Không thể truy cập trang {page_url}")
                continue
            
            job_elements = soup.select('.item-job-box')
            page_job_urls = []
            
            for job_element in job_elements:
                title_element = job_element.select_one('.field-job-title a')
                if title_element and 'href' in title_element.attrs:
                    job_url = title_element['href']
                    if not job_url.startswith('http'):
                        job_url = self.base_url + job_url
                    page_job_urls.append(job_url)
            
            print(f"Đã tìm thấy {len(page_job_urls)} việc làm trên trang {page}")
            
            page_jobs = self.crawl_multiple_urls(page_job_urls, output_folder, page_number=page)
            all_jobs.extend(page_jobs)
            
            if page < num_pages:
                delay = random.uniform(3, 5)
                print(f"Chờ {delay:.2f} giây trước khi crawl trang tiếp theo...")
                time.sleep(delay)
        
        print(f"Đã hoàn thành crawl tổng cộng {len(all_jobs)} việc làm từ {num_pages} trang.")
        return all_jobs

if __name__ == "__main__":
    crawler = TimViecCrawler()
    
    num_pages = int(input("Nhập số trang cần crawl: ") or "5")
    output_folder = input("Nhập thư mục để lưu kết quả (mặc định: job_details): ") or "job_details"
        
    jobs = crawler.crawl_direct_from_web(num_pages, output_folder)
        
    print(f"Đã crawl và lưu tổng cộng {len(jobs)} việc làm.")
