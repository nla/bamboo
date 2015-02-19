[@page title="${crawl.name}"]

[#macro indexingProgress todo]
    [#if todo > 0]
        ${todo} WARC files require indexing
        [@progress now=(warcCount - todo) max=warcCount /]

    [#else]
        Complete
    [/#if]
[/#macro]

<table class="table">
    <tr><th class="col-md-2">State</th><td>${crawl.stateName()}</td></tr>
    <tr><th class="col-md-2">WARC files</th><td>${warcCount}</td></tr>
    <tr><th class="col-md-2">CDX Indexing:</th><td>[@indexingProgress warcsToBeCdxIndexed /]</td></tr>
    <tr><th class="col-md-2">Solr Indexing:</th><td>[@indexingProgress warcsToBeSolrIndexed /]</td></tr>
</table>
[/@page]