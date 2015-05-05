[@page title="WARC files in ${crawl.name}"]

<h3>WARC files in <a href="crawls/${crawl.id?c}">${crawl.name}</a></h3>

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
            <td>${warc.filename}</td>
            <td>${warc.records}</td>
            <td>${si(warc.size)}</td>
            <td><a href="warcs/${warc.id?c}">Download</a></td>
        </tr>
    [/#list]
    </tbody>
</table>

[@pagination warcsPager.currentPage warcsPager.lastPage /]

[/@page]