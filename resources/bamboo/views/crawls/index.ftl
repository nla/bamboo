[@page title="Crawls"]

[#if crawls.isEmpty()]
    <p>No crawls found.</p>
[#else]
    [#include "/crawls/_list.ftl"]
    [@pagination crawlsPager.currentPage crawlsPager.lastPage /]
[/#if]

<p><a href="import" class="btn btn-primary">Import Crawl from Heritrix</a></p>

[/@page]