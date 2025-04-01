(() => {
  const fetchJobUrls = async (page) => {
    const url = `https://jobsgo.vn/viec-lam.html?sort=created&page=${page}`;
    try {
      const response = await fetch(url);
      const text = await response.text();
      const parser = new DOMParser();
      const doc = parser.parseFromString(text, "text/html");
      const jobElements = doc.querySelectorAll(
        "body > section.section.wrap-1 > div > div > div > div > div > div > div.col-sm-9 > div.row-ajax > div.clearfix.colorgb-carousel-bk.v2 > div.item-bk.mrg-top-10 > div > div > div > article > div > div > div > div.brows-job-position > h3 > a"
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
      const company =
        doc.querySelector(
          "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-4.job-detail-col-2 > div > div > div > div.profile-cover > div.media > div.media-body > h2 > a"
        ) ??
        doc.querySelector(
          "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-4.job-detail-col-2 > div > div > div > div.media > div.media-body > h2 > a"
        );
      const job_location =
        doc.querySelector(
          "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-4.job-detail-col-2 > div > div > div > div.company-info.text-grey > div.padd-20.padd-t-0 > div:last-child > span"
        ) ??
        doc.querySelector(
          "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-4.job-detail-col-2 > div > div > div > div.company-info.text-grey > div.padd-20.padd-0 > div:last-child > span"
        );
      const job_skills =
        doc.querySelectorAll(
          "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div:nth-child(6) > div:nth-child(3) > div > div > a"
        ).length === 0
          ? doc.querySelectorAll(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div:nth-child(6) > div:nth-child(2) > div > div > a"
            )
          : doc.querySelectorAll(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div:nth-child(6) > div:nth-child(3) > div > div > a"
            );

      var job = {
        job_title:
          doc
            .querySelector(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div.media.stack-media-on-mobile.text-left.content-group.pb-0.mrg-top-10 > div.media-body-2 > h1"
            )
            ?.innerHTML.trim() ?? "",
        company: company?.innerHTML.trim() ?? "",
        job_location: job_location?.textContent.trim() ?? "",
        search_city:
          job_location?.textContent.trim().split(",").pop().trim() ?? "",
        job_level:
          doc
            .querySelector(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div.row.teks-grid > div:nth-child(2) > div > p:nth-child(3)"
            )
            ?.innerHTML.trim() ?? "",
        job_type:
          doc
            .querySelector(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div.row.teks-grid > div:nth-child(1) > div > p:nth-child(3)"
            )
            ?.textContent.trim() ?? "",
        job_skills:
          Array.from(job_skills)
            .map((a) => a?.textContent.trim())
            .join(", ") ?? "",
        first_seen:
          doc
            .querySelector(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div.row.teks-grid > div:nth-child(5) > div > p:nth-child(3)"
            )
            ?.innerHTML.trim() ?? "",
        search_country: "Việt Nam",
        search_position: job_location
          ? job_location?.textContent.trim().split(",").pop().trim()
          : "",
        last_processed_time: "",
        got_summary: "TRUE",
        got_ner: "TRUE",
        is_being_worked: "FALSE",
        branch:
          Array.from(
            doc.querySelectorAll(
              "body > section.section.colorgb-single.wrap-1.padd-top-0 > div > div > div > div > div.col-sm-8.pr0.job-detail-col-1 > div > div > div:nth-child(6) > div:nth-child(2) > div > div > a"
            )
          )
            .map((item) => item?.textContent)
            .join(", ") ?? "",
      };
      return job;
    } catch (error) {
      console.error(`Lỗi khi tải trang 2 ${url}:`, error);
      return null;
    }
  };

  const saveToCSV = (data, page) => {
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
    a.download = `job_page_${page}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  (async () => {
    for (let page = 201; page <= 300; page++) {
      const urls = await fetchJobUrls(page);
      const jobPromises = urls.map(fetchPageAsDocument);
      const jobs = (await Promise.all(jobPromises)).filter(Boolean);
      saveToCSV(jobs, page);
    }
  })();
})();
