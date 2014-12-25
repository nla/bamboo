[@page title="Crawl Series"]
<table class="table">
    <thead>
        <tr><td>Name</td><td>Path</td></tr>
    </thead>
    <tbody>
        [#list seriesList as series]
            <tr><td>${series.name}</td><td>${series.path}</td></tr>
        [/#list]
    </tbody>
</table>
<a href="series/new" class="btn btn-primary">New Crawl Series</a>
[/@page]