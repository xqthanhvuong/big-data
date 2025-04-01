(async () => {
    const delay = (ms) => new Promise(res => setTimeout(res, ms));
    let jobLinks = [];
    let jobs = [];

    const fetchHTML = async (url) => {
        let response = await fetch(url);
        let htmlText = await response.text();
        let parser = new DOMParser();
        return parser.parseFromString(htmlText, "text/html");
    };

    for (let i = 1; i <= 1; i++) {
        let url = `https://vieclam24h.vn/tim-kiem-viec-lam-nhanh?page=${i}&sort_q=priority_max%2Cdesc`;
        console.log(`Đang lấy danh sách tin từ: ${url}`);

        let doc = await fetchHTML(url);
        let container = doc.querySelector('.flex.flex-col.gap-3.sm_cv\\:gap-2');
        let links = container ? [...container.querySelectorAll('a')].map(a => a.href.startsWith('http') ? a.href : 'https://vieclam24h.vn' + a.href) : [];

        jobLinks.push(...links);
        await delay(1000);
    }

    console.log(`Tổng số tin tuyển dụng thu thập: ${jobLinks.length}`);

    for (let jobUrl of jobLinks) {
        console.log(`Đang lấy thông tin từ: ${jobUrl}`);
        let doc = await fetchHTML(jobUrl);

        let getText = (selector) => doc.querySelector(selector)?.innerText.trim() || "";

        // Lấy địa chỉ
        let addressLabel = doc.querySelector("#test .svicon-map-marker-alt");
        let job_location = "";
        if (addressLabel) {
            let addressContainer = addressLabel.closest("h4");
            if (addressContainer) {
                let addressDiv = addressContainer.querySelector("div.text-14.text-se-grey-48.font-semibold");
                if (addressDiv) {
                    job_location = addressDiv.innerText.trim();
                }
            }
        }
        let search_city = job_location.split(',').pop().trim();
        let search_country = "Việt Nam";

        // Lấy cấp bậc công việc (job_level)
        let levelLabel = [...doc.querySelectorAll(".svicon-medal")].find(el => el.closest("div"));
        let job_level = levelLabel ? levelLabel.closest("div").querySelector("p.text-14")?.innerText.trim() || "" : "";

        // Lấy ngành nghề (branch)
        let branchContainer = [...doc.querySelectorAll(".svicon-suitcase")].find(el => el.closest("div"));
        let branch = "";
        if (branchContainer) {
            let branchDiv = branchContainer.closest("div").querySelector("p.text-14.text-se-accent");
            if (branchDiv) {
                branch = [...branchDiv.querySelectorAll("a")].map(a => a.innerText.trim()).join("").split("/").join(", ");
            }
        }

        let job = {
            job_title: getText("h1.font-semibold.text-18.md\\:text-24"),
            company: getText("a[href*='/danh-sach-tin-tuyen-dung'] h2"),
            job_location,
            search_city,
            search_country,
            job_level,
            branch,
            job_skills: getText("div.jsx-5b2773f86d2f74b.mb-4.md\\:mb-8"),
            last_processed_time: getText(".svicon-calendar-day ~ span .font-semibold"),
            url: jobUrl
        };

        jobs.push(job);
        await delay(1000);
    }

    console.log("Dữ liệu đã thu thập:", jobs);
// Sau khi thu thập xong dữ liệu, gọi hàm này:
saveToFile(jobs, 'jobs_data.json');
})();
const saveToFile = (data, filename) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
};



