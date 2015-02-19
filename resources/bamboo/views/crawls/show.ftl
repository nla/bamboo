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
    <!-- <tr><th class="col-md-2">State</th><td>${crawl.stateName()}</td></tr> -->
    <tr><th class="col-md-2">Crawl Series</th><td><a href="series/${series.id}">${series.name}</a></td></tr>
    <tr><th class="col-md-2">WARC files</th><td>${crawl.warcFiles} warcs, <abbr title="${crawl.warcSize} bytes">${si(crawl.warcSize)}B</abbr> (compressed)</td></tr>
    <tr><th class="col-md-2">CDX Indexing</th><td>[@indexingProgress warcsToBeCdxIndexed /]</td></tr>
    <tr><th class="col-md-2">Solr Indexing</th><td>[@indexingProgress warcsToBeSolrIndexed /]</td></tr>
</table>
[/@page]