[@page title="${collection.name}"]

<table class="table">
    <tr><th>WARC Records</th><td>${collection.records} records, <abbr title="${collection.recordBytes} bytes">${si(collection.recordBytes)}B</abbr> (uncompressed)</td></tr>
    <tr><th class="col-md-2">CDX URL</th><td>${collection.cdxUrl!"None"}</td></tr>
    <tr><th class="col-md-2">Solr URL</th><td>${collection.solrUrl!"None"}</td></tr>
</table>

<a href="collections/${collection.id}/edit" class="btn btn-default">Edit</a>

[/@page]