<table class="table">
    <thead>
        <tr>
            <td>Name</td>
            <td>Size</td>
            <td>Finished</td>
            <td>Alerts</td>
        </tr>
    </thead>
    <tbody>
        [#list crawls as crawl]
            <tr>
                <td><a href="crawls/${crawl.id}">${crawl.name}</a></td>
                <td>${si(crawl.recordBytes)}B</td>
                <td>1/1/2013</td>
                <td></td>
            </tr>
        [/#list]
    </tbody>
</table>