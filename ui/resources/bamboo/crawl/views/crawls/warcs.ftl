[@page title="${titlePrefix!} WARC files in ${crawl.name}"]

<h3>${titlePrefix!} WARC files in <a href="crawls/${crawl.id?c}">${crawl.name}</a>
    <a class="btn btn-primary pull-right" href="crawls/${crawl.id?c}/warcs/download">Download All (${si(crawl.warcSize)}B)</a>
</h3>

<table class="table">
    <thead>
        <tr>
            <td>Filename</td>
            <td>Records</td>
            <td>Size</td>
        </tr>
    </thead>
    <tbody>
    [#list warcs as warc]
        <tr>
            <td><a href="warcs/${warc.id?c}/details">${warc.filename}</a></td>
            <td>${warc.records}</td>
            <td>${si(warc.size)}</td>
            <td><a href="warcs/${warc.id?c}/cdx">CDX</a> | <a href="warcs/${warc.id?c}">Download</a></td>
        </tr>
    [/#list]
    </tbody>
</table>

[@pagination warcsPager.currentPage warcsPager.lastPage /]

[/@page]