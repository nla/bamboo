[@page title="CDX ${cdx.path}"]
<table>
    <tr>
        <th>Documents</th>
        <td>${cdx.totalDocs}</td>
    </tr>
    <tr>
        <th>Uncompressed Size</th>
        <td><abbr title="${cdx.totalBytes} bytes">${si(cdx.totalBytes)}B</abbr></td>
    </tr>
</table>
<form action="cdx/${cdx.id}/calcstats" method="POST">
    <button type="submit">Calculate Statistics</button>
</form>
<h3>Crawls</h3>
<ul>
    [#list crawls as crawl]
    <li><a href="crawl/${crawl.id}">${crawl.name}</a></li>
    [/#list]
</ul>
[/@page]