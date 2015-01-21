[@page title="Import Crawl"]

<form method="POST">
    <input name="csrfToken" value="${csrfToken}" type="hidden">
    <label for="heritrixJob">Heritrix Job:</label>
    <select id="heritrixJob" name="heritrixJob" class="chosen">
        [#list jobs as job]
            <option>${job.name}</option>
        [/#list]
    </select><br>
    <label for="crawlSeriesId">Crawl Series:</label>
    <select id="crawlSeriesId" name="crawlSeriesId" class="chosen">
        [#list allCrawlSeries as series]
            <option value="${series.id}"[#if series.id == selectedCrawlSeriesId!-1] selected[/#if]>${series.name}</option>
        [/#list]
    </select><br>
    <button type="submit">Import</button>
</form>

<script src="webjars/jquery/2.1.3/jquery.min.js"></script>
<script src="webjars/chosen/1.2.0/chosen.jquery.min.js"></script>
<link rel="stylesheet" href="webjars/chosen/1.2.0/chosen.min.css">
<script>$(".chosen").chosen();</script>
[/@page]