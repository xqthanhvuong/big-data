
(async () => {
    const fetchHTML = async (url) => {
        let response = await fetch(url);
        let htmlText = await response.text();
        let parser = new DOMParser();
        return parser.parseFromString(htmlText, "text/html");
    };

    // Duyệt từ trang 1 đến 50 song song
    const totalPages = 50;
    const pageNumbers = Array.from({ length: totalPages }, (_, i) => i + 3151);

    // Lấy danh sách job links từ tất cả các trang cùng lúc
    const jobPages = await Promise.all(pageNumbers.map(async (i) => {
        let url = `https://viecoi.vn/tim-viec/all.html?page=${i}`;
        console.log(`Đang lấy danh sách tin từ: ${url}`);
        let doc = await fetchHTML(url);
        let container = doc.querySelector("#banner > div.main-content > div > div > div.col-md-9.col-md-push-3.content_job > div > div.jobs_container");
        return container ? [...container.querySelectorAll("a[id^='link_job_'].line-clamp-2.title_container")].map(a => a.href) : [];
    }));

    // Gộp danh sách job từ tất cả các trang
    let allJobLinks = jobPages.flat();
    console.log(`Tìm thấy ${allJobLinks.length} tin tuyển dụng.`);

    // Lấy chi tiết từng job song song
    const jobs = await Promise.all(allJobLinks.map(async (jobUrl) => {
        console.log(`Đang lấy thông tin từ: ${jobUrl}`);
        let doc = await fetchHTML(jobUrl);
        let getText = (selector) => doc.querySelector(selector)?.innerText.trim() || "";

        let addressLabel = doc.querySelector("#tab-sidebar-overview > div.info-company-component > ul > li:nth-child(3)");
        let job_location = addressLabel ? addressLabel.innerText.replace("Địa chỉ", "").trim() : "";
        let search_city = getText("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-4.col-xs-12 > div.info-item > div > a");
        let search_country = "Việt Nam";

        let branchContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-8.col-xs-12 > div.info-item > div");
        let listBranch = branchContainer ? [...branchContainer.querySelectorAll("a")].map(a => a.innerText) : [];
        let branch = listBranch.join(", ").replace("/", " -");

        let levelContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li:nth-child(1) > div.info-item > p.details-info");
        let job_level = levelContainer ? levelContainer.innerText.trim() : "";

        let jobSkillsContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-skill-details > div > div");
        let jobSkills = jobSkillsContainer ? [...jobSkillsContainer.querySelectorAll("a")].map(a => a.innerText) : [];
        let job_skills = jobSkills.join(", ");

        let jobTitleContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.jobs-head-details > div.block-archive-inner.jobs-border-buttom.vo-jobs-header-top.pd-bottom-mb-0.show-web > div.vo-header-left > div > div.title-wapper");
        jobTitleContainer?.querySelector("span")?.remove();

        return {
            job_title: jobTitleContainer?.innerText.replace("Tuyển dụng", "").trim() || "",
            company: getText("#tab-sidebar-company > div.company-header > div > h2 > a"),
            job_location,
            search_city,
            search_country,
            job_level,
            branch,
            job_skills,
            last_processed_time: getText("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li:nth-child(2) > div.info-item > p.details-info"),
            url: jobUrl
        };
    }));

    console.log(jobs);
    console.log(`Tổng số tin tuyển dụng thu thập: ${jobs.length}`);saveToFile(jobs, 'jobs_data_viecoi45.json');
})();const saveToFile = (data, filename) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
};