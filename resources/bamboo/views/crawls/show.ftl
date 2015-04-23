[@page title="Crawl: ${crawl.name}"]

[#macro indexingProgress todo]
    [#if todo > 0]
        ${todo} warcs require indexing
        [@progress now=(crawl.warcFiles - todo) max=crawl.warcFiles /]

    [#else]
        Complete
    [/#if]
[/#macro]

[#if crawl.state == 1]
    <div class="alert alert-info" role="alert">
        This crawl is currently being imported and the statistics below may be incomplete.
    </div>
[/#if]

<table class="table">
    <colgroup>
        <col class="col-md-2">
    </colgroup>
    <!-- <tr><th class="col-md-2">State</th><td>${crawl.stateName()}</td></tr> -->
    <tr><th>Crawl Series</th><td><a href="series/${series.id}">${series.name}</a></td></tr>
    <tr><th>WARC Records</th><td>${crawl.records} records, <abbr title="${crawl.recordBytes} bytes">${si(crawl.recordBytes)}B</abbr> (uncompressed)</td></tr>
    <tr><th>WARC Files</th><td>${crawl.warcFiles} warcs, <abbr title="${crawl.warcSize} bytes">${si(crawl.warcSize)}B</abbr> (compressed)</td></tr>
    [#if corruptWarcs > 0]
      <tr><th>Corrupt WARC files</th><td>${corruptWarcs} warc files were unreadable due to format corruption during indexing</td></tr>
    [/#if]
    <tr><th>CDX Indexing</th><td>[@indexingProgress warcsToBeCdxIndexed /]</td></tr>
    <tr><th>Solr Indexing</th><td>[@indexingProgress warcsToBeSolrIndexed /]</td></tr>
</table>

<!-- <a class="btn btn-primary" href="crawls/${crawl.id}/edit">Edit Crawl</a> -->

[/@page]