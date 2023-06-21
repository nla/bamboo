[@page title="${warc.filename}"]

<h3>${warc.filename}</h3>

<table class="table">
    <colgroup>
        <col class="col-md-2">
    </colgroup>
    <tr><th>Crawl</th><td><a href="crawls/${crawl.id?c}">${crawl.name}</a></td></tr>
    <tr><th>State</th><td>${state}</td></tr>
    <tr><th>Records</th><td><a href="warcs/${warc.id?c}/cdx">${warc.records} records</a></td></tr>
    <tr><th>File Size</th><td><abbr title="${warc.size} bytes">${warc.displaySize}</abbr></td></tr>
    <tr><th>Path</th><td>${warc.path!}</td></tr>
    <tr><th>SHA-256 Digest</th><td>${warc.sha256!}</td></tr>
    <tr><th>Software</th><td>${warc.software!}</td></tr>
</table>

<div class="action-buttons">
    <form action="warcs/${warc.id?c}/reindex" method="post" style="display: inline">
        <button type="submit" class="btn btn-default">Reindex</button>
    </form>
    [#if warc.stateId != -4]
        <form action="warcs/${warc.id?c}/delete" method="post" style="display: inline; float: right">
            <button type="submit" class="btn btn-danger" onclick="return confirm('Are you certain you wish to delete ${warc.filename}?')">Delete</button>
        </form>
    [/#if]
</div>

[/@page]