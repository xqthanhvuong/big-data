(() => {
  const fetchJobUrls = async (page) => {
    const url = `https://viecoi.vn/tim-viec/all.html?page=${page}`;
    try {
      const response = await fetch(url);
      const text = await response.text();
      const parser = new DOMParser();
      const doc = parser.parseFromString(text, "text/html");
      const jobElements = doc.querySelectorAll(
        "#banner > div.main-content > div > div > div.col-md-9.col-md-push-3.content_job > div > div.jobs_container > div > div > div.jobs-image-item > a"
      );

      const links = Array.from(jobElements)
        .map((el) => el.href)
        .filter((url) => url !== null);
      console.log(" page");
      return links;
    } catch (error) {
      console.error(`Lỗi khi tải trang 1 ${url}:`, error);
      return [];
    }
  };

  const fetchPageAsDocument = async (url) => {
    try {
      const response = await fetch(url);
      const html = await response.text();
      const parser = new DOMParser();
      const doc = parser.parseFromString(html, "text/html");
      const company = doc.querySelector(
        "#tab-sidebar-company > div.company-header > div > h2 > a"
      );
      const job_location = doc.querySelector(
        "#tab-sidebar-overview > div.info-company-component > ul > li:nth-child(3) > div.info-item > p.details-info.flip-2"
      );
      const job_skills = doc.querySelector(
        "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-skill-details > div > div"
      );

      var job = {
        job_title:
          doc
            .querySelector(
              "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.jobs-head-details > div.block-archive-inner.jobs-border-buttom.vo-jobs-header-top.pd-bottom-mb-0.show-web > div.vo-header-left > div > div.title-wapper > h1"
            )
            ?.textContent.trim() ?? "",
        company: company?.textContent.trim() ?? "",
        job_location: job_location?.textContent.trim() ?? "",
        search_city:
          doc
            .querySelector(
              "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-4.col-xs-12 > div.info-item > div > a"
            )
            ?.textContent.trim() ?? "",
        job_level:
          doc
            .querySelector(
              "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li:nth-child(1) > div.info-item > p.details-info > a"
            )
            ?.textContent.trim() ?? "",
        job_type: "",
        job_skills: job_skills?.textContent.trim() ?? "",
        first_seen: "",
        search_country: "Việt Nam",
        search_position: "",
        last_processed_time:
          doc
            .querySelector(
              "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li:nth-child(2) > div.info-item > p.details-info"
            )
            ?.textContent.trim() ?? "",
        got_summary: "TRUE",
        got_ner: "TRUE",
        is_being_worked: "FALSE",
        branch:
          doc.querySelector(
            "#detailsjob-page > div.container.js-container > div.row > div.col-md-8.pd-mb-0.col-md-70 > div.block-jobs-warrper > div > div.block-archive-inner.block-info-jobs.jobs-border-buttom.jobs-insights-details > ul > div > li.list-item.col-md-8.col-xs-12 > div.info-item > div"
          )?.textContent.trim() ?? "",
      };
      return job;
    } catch (error) {
      console.error(`Lỗi khi tải trang 2 ${url}:`, error);
      return null;
    }
  };

  const saveToCSV = (data, name) => {
    if (data.length === 0) return;

    const headers = Object.keys(data[0]).join(",");
    const rows = data.map((job) =>
      Object.values(job)
        .map((value) => (value ? `"${value}"` : ""))
        .join(",")
    );
    const csvContent = [headers, ...rows].join("\n");

    const blob = new Blob([csvContent], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${name}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  (async () => {
    let allJobs = [];
    for (let page = 1501; page <= 2000; page++) {
      const urls = await fetchJobUrls(page);
      const jobPromises = urls.map(fetchPageAsDocument);
      const jobs = (await Promise.all(jobPromises)).filter(Boolean);

      allJobs.push(...jobs);

      if (page % 20 === 0) {
        // Cứ sau mỗi 5 trang thì ghi vào CSV
        saveToCSV(allJobs, `jobs_${page - 19}_to_${page}.csv`);
        allJobs = []; // Reset danh sách
      }
    }

    // Nếu còn dữ liệu sót lại (khi số trang không chia hết cho 5)
    if (allJobs.length > 0) {
      saveToCSV(allJobs, `jobs_remaining.csv`);
    }
  })();
})();
