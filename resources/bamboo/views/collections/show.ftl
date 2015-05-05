[@page title="${collection.name}"]

<h3>${collection.name} <a href="collections/${collection.id?c}/edit" class="btn btn-default pull-right">Edit</a></h3>

<div class="row">
    <div class="col-md-8">
        <div class="description well">
            <div class="panel-body">
                [#noescape]${descriptionHtml!"No description."}[/#noescape]
            </div>
        </div>
    </div>
    <div class="col-md-4">
        <table class="table">
            <colgroup>
                <col class="col-md-2">
            </colgroup>
            <tr>
                <th>Size</th>
                <td>
                    ${collection.records} records; <abbr title="${collection.recordBytes} bytes">${si(collection.recordBytes)}B</abbr><br>
                </td></tr>
            <tr>
            <tr><th>CDX</th><td>${collection.cdxUrl!"None"}</td></tr>
            <tr><th>Solr</th><td>${collection.solrUrl!"None"}</td></tr>
        </table>
    </div>
</div>

[/@page]