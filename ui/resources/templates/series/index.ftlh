[#-- @ftlvariable name="seriesPager" type="org.springframework.data.domain.Page<bamboo.crawl.Series>" --]
[@page title="Series"]
<h3>All ${(agency.abbreviation)!""} Crawl Series [#if auth.hasRole("agadmin")]<a href="series/new" class="btn btn-primary pull-right">New Series</a>[/#if]</h3>

<table class="table">
    <thead>
    <tr>
        [#if !agency??]<th style="width: 1px">Agency</th>[/#if]
        <th>Name</th>
        <th>Size</th>
        <th>Crawls</th>
    </tr>
    </thead>
    <tbody>
        [#list seriesList as series]
            <tr>
                [#if !agency??]<td>${(series.agency.abbreviation)!""}</td>[/#if]
                <td><a href="series/${series.id?c}">${series.name}</a></td>
                <td>${si(series.recordBytes)}B</td>
                <td>${series.crawlCount}</td>
            </tr>
        [/#list]
    </tbody>
</table>
[@pagination current=(seriesPager.getNumber() + 1) last=seriesPager.getTotalPages() /]

[/@page]