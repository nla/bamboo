[@page title="WARC files queued for ${queueName}"]

<h3>${warcsPager.totalItems} WARC files queued for ${queueName}</h3>

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
        </tr>
    [/#list]
    </tbody>
</table>

[@pagination warcsPager.currentPage warcsPager.lastPage /]

[/@page]