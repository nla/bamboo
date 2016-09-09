[@page title="${warc.filename}"]

<h3>${warc.filename}</h3>

<table class="table">
    <colgroup>
        <col class="col-md-2">
    </colgroup>
    <tr><th>Crawl</th><td><a href="crawls/${crawl.id?c}">${crawl.name}</a></td></tr>
    <tr><th>State</th><td>${state}</td></tr>
    <tr><th>Records</th><td><a href="warcs/${warc.id?c}/cdx">${warc.records} records</a>; <abbr title="${warc.recordBytes} bytes">${si(warc.recordBytes)}B</abbr></td></tr>
    <tr><th>File Size</th><td><abbr title="${warc.size} bytes">${si(warc.size)}B</abbr></td></tr>
    <tr><th>Path</th><td>${warc.path!}</td></tr>
    <tr><th>SHA-256 Digest</th><td>${warc.sha256!}</td></tr>
</table>

[/@page]