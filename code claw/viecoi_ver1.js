(async () => {
    const delay = (ms) => new Promise(res => setTimeout(res, ms));
    let allJobLinks = [];
    let jobs = [];
    
    const fetchHTML = async (url) => {
        let response = await fetch(url);
        let htmlText = await response.text();
        let parser = new DOMParser();
        return parser.parseFromString(htmlText, "text/html");
    };
    
    for (let i = 1; i <= 1; i++) {
        let url = `https://viecoi.vn/tim-viec/all.html?page=${i}`;
        console.log(`Đang lấy danh sách tin từ: ${url}`);
        
        let doc = await fetchHTML(url);
        let container = doc.querySelector("#banner > div.main-content > div > div > div.col-md-9.col-md-push-3.content_job > div > div.jobs_container");

        let jobLinks = container ? [...container.querySelectorAll("a[id^='link_job_'].line-clamp-2.title_container")] : [];

        let jobU = jobLinks.map(a => a.href);
        
        
        allJobLinks.push(...jobU);
        await delay(100);
    }

    for(let jobUrl of allJobLinks){
        console.log(`Đang lấy thông tin từ: ${jobUrl}`);
        let doc = await fetchHTML(jobUrl);
        let getText = (selector) => doc.querySelector(selector)?.innerText.trim() || "";
        let addressLabel = doc.querySelector("#tab-sidebar-overview > div.info-company-component > ul > li:nth-child(3)");
        let job_location = addressLabel ? addressLabel.innerText.replace("Địa chỉ","").trim() : "";
        let search_city = getText("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-4.col-xs-12 > div.info-item > div > a");
        let search_country = "Việt Nam";

        let branchContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-8.col-xs-12 > div.info-item > div");
        let listBranch = branchContainer ? [...branchContainer.querySelectorAll("a")].map(a=> a.innerText) : [];
        let branch = listBranch.join(", ").replace("/"," -");
        
        let levelContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li:nth-child(1) > div.info-item > p.details-info");

        let job_level = levelContainer ? levelContainer.innerText.trim() : "";

        let jobSkillsContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-skill-details > div > div");
        let jobSkills = jobSkillsContainer ? [...jobSkillsContainer.querySelectorAll("a")].map(a=> a.innerText) : [];
        let job_skills = jobSkills.join(", ");


        let jobTitleContainer = doc.querySelector("#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.jobs-head-details > div.block-archive-inner.jobs-border-buttom.vo-jobs-header-top.pd-bottom-mb-0.show-web > div.vo-header-left > div > div.title-wapper");
        jobTitleContainer.querySelector("span")?.remove();

        let job = {
            job_title: jobTitleContainer.innerText?.replace("Tuyển dụng","").trim(),
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

        jobs.push(job);
        await delay(100);
    }
    console.log(jobs);
    
    console.log(`Tổng số tin tuyển dụng thu thập: ${jobLinks.length}`);
})();


