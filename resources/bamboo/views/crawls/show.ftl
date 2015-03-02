[@page title="Crawl: ${crawl.name}"]

[#macro indexingProgress todo]
    [#if todo > 0]
        ${todo} warcs require indexing
        [@progress now=(crawl.warcFiles - todo) max=crawl.warcFiles /]

    [#else]
        Complete
    [/#if]
[/#macro]

<table class="table">
    <colgroup>
        <col class="col-md-2">
    </colgroup>
    <!-- <tr><th class="col-md-2">State</th><td>${crawl.stateName()}</td></tr> -->
    <tr><th>Crawl Series</th><td><a href="series/${series.id}">${series.name}</a></td></tr>
    <tr><th>WARC Records</th><td>${crawl.records} records, <abbr title="${crawl.recordBytes} bytes">${si(crawl.recordBytes)}B</abbr> (uncompressed)</td></tr>
    <tr><th>WARC Files</th><td>${crawl.warcFiles} warcs, <abbr title="${crawl.warcSize} bytes">${si(crawl.warcSize)}B</abbr> (compressed)</td></tr>
    <tr><th>CDX Indexing</th><td>[@indexingProgress warcsToBeCdxIndexed /]</td></tr>
    <tr><th>Solr Indexing</th><td>[@indexingProgress warcsToBeSolrIndexed /]</td></tr>
</table>

<!-- <a class="btn btn-primary" href="crawls/${crawl.id}/edit">Edit Crawl</a> -->

[/@page]