[@page title="Crawls"]

<h3>All Crawls
    <a href="import" class="btn btn-primary pull-right">Import from Heritrix</a>
    <a href="crawls/new" class="btn btn pull-right">Upload</a>
</h3>

[#if !crawls?has_content]
    <p>No crawls found.</p>
[#else]
    <table class="table">
        <thead>
        <tr>
            <td>Name</td>
            <td>Series</td>
            <td>Size</td>
            <td>Finished</td>
        </tr>
        </thead>
        <tbody>
        [#list crawls as crawl]
        <tr>
            <td><a href="crawls/${crawl.id?c}">${crawl.name}</a></td>
            <td>[#if crawl.crawlSeriesId??]<a href="series/${crawl.crawlSeriesId?c}">${crawl.seriesName}</a>[/#if]</td>
            <td>${crawl.warcDisplaySize}</td>
            <td>${(crawl.endTime?date)!""}</td>
        </tr>
        [/#list]
        </tbody>
    </table>
    [@pagination crawlsPager.currentPage crawlsPager.lastPage /]
[/#if]

[/@page]