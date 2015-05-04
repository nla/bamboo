[@page title="Crawls"]

<h3>All Crawls <a href="import" class="btn btn-primary pull-right">Import from Heritrix</a></h3>

[#if crawls.isEmpty()]
    <p>No crawls found.</p>
[#else]
    <table class="table">
        <thead>
        <tr>
            <td>Name</td>
            <td>Series</td>
            <td>Size</td>
            <td>Finished</td>
            <td>Alerts</td>
        </tr>
        </thead>
        <tbody>
        [#list crawls as crawl]
        <tr>
            <td><a href="crawls/${crawl.id}">${crawl.name}</a></td>
            <td><a href="series/${crawl.crawlSeriesId}">${crawl.seriesName}</a></td>
            <td>${si(crawl.recordBytes)}B</td>
            <td>1/1/2013</td>
            <td></td>
        </tr>
        [/#list]
        </tbody>
    </table>
    [@pagination crawlsPager.currentPage crawlsPager.lastPage /]
[/#if]

[/@page]