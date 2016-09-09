[@page title="Task Queue"]
<table class="table">
    <thead>
        <tr><td>Name</td><td>Path</td></tr>
    </thead>
    <tbody>
    [#list seriesList as series]
    <tr>
        <td><a href="series/${series.id?c}">${series.name}</a></td>
        <td>${series.path}</td>
    </tr>
    [/#list]
    </tbody>
</table>
[/@page]