[@page title="Series: ${series.name}"]

<table class="table">
    <tr><th class="col-md-2">WARC files</th><td>${series.warcFiles} warcs, <abbr title="${series.warcSize} bytes">${si(series.warcSize)}B</abbr> (compressed)</td></tr>
</table>

<h2>Crawls</h2>

[#include "/crawls/_list.ftl"]

<p></p><a href="import?crawlSeries=${series.id}" class="btn btn-primary">Import Crawl from Heritrix</a></p>

[/@page]