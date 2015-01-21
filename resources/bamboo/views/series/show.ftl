[@page title="${series.name}"]

<table>
    <tr><td>Name:</td><td>${series.name}</td></tr>
</table>

<h2>Crawls</h2>

[#include "/crawls/_list.ftl"]

<p></p><a href="import?crawlSeries=${series.id}" class="btn btn-primary">Import Crawl from Heritrix</a></p>

[/@page]