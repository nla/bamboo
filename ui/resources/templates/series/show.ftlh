[@page title="${series.name}"]

<h3>${series.name}
    [#if auth.hasRole("panadmin")]
        <a class="btn btn-default pull-right" href="series/${series.id?c}/edit">Edit</a>
    [/#if]
</h3>

<div class="row">
    <div class="col-md-8">
        <div class="description well">
            <div class="panel-body">
                ${(descriptionHtml!"No description.")?no_esc}
            </div>
        </div>
    </div>
    <div class="col-md-4">
        <table class="table">
            <colgroup>
                <col class="col-md-2">
            </colgroup>
            [#if agency?has_content]
                <tr>
                    <th>Agency</th>
                    <td>${agency.name}</td>
                </tr>
            [/#if]
            <tr>
                <th>Size</th>
                <td>
                    ${series.records} records; <abbr title="${series.recordBytes} bytes">${si(series.recordBytes)}B</abbr><br>
                    ${series.warcFiles} warc files; <abbr title="${series.warcSize} bytes">${si(series.warcSize)}B</abbr>
                </td>
            </tr>
            [#if auth.hasRole("panadmin")]
                <tr><th>Collections</th>
                    <td>
                        [#if collections?size = 0]
                            None
                        [#else]
                            <ul>
                                [#list collections as collection]
                                    <li><a href="collections/${collection.id?c}">${collection.name}</a>
                                        [#if collection.urlFilters?has_content] (filtered)
                                        <div class="text-indent-8">
                                            <pre>${collection.urlFilters}</pre>
                                        </div>
                                        [/#if]
                                    </li>
                                [/#list]
                            </ul>
                        [/#if]
                    </td>
                </tr>
            [/#if]
        </table>
    </div>
</div>


<h4>Crawls
    [#if auth.hasRole("panadmin")]
        [#if (series.path!"") != ""]
            <a href="import?crawlSeries=${series.id?c}" class="btn btn-primary pull-right">Import from Heritrix</a>
        [/#if]
    [/#if]
    <a class="btn btn-default pull-right" href="crawls/new?crawlSeries=${series.id?c}">Upload</a>
</h4>


<table class="table">
    <thead>
    <tr>
        <td>Name</td>
        <td>Size</td>
        <td>Finished</td>
    </tr>
    </thead>
    <tbody>
    [#list crawlList as crawl]
    <tr>
        <td><a href="crawls/${crawl.id?c}">${crawl.name}</a></td>
        <td>${si(crawl.recordBytes)}B</td>
        <td>${(crawl.endTime?date)!""}</td>
    </tr>
    [/#list]
    </tbody>
</table>
[@pagination crawlPager.currentPage crawlPager.lastPage /]

[/@page]