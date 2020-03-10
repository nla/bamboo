[@page title="Series"]
<h3>All ${(agency.abbreviation)!""} Series [#if auth.hasRole("panadmin")]<a href="series/new" class="btn btn-primary pull-right">New Series</a>[/#if]</h3>

<table class="table">
    <thead>
        <tr><td>Name</td><td>Size</td><td>Crawls</td></tr>
    </thead>
    <tbody>
        [#list seriesList as series]
            <tr>
                <td><a href="series/${series.id?c}">${series.name}</a></td>
                <td>${si(series.recordBytes)}B</td>
                <td>${series.crawlCount}</td>
            </tr>
        [/#list]
    </tbody>
</table>
[@pagination seriesPager.currentPage seriesPager.lastPage /]

[/@page]