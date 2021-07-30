[#-- @ftlvariable name="pandasInstance" type="bamboo.pandas.PandasInstance" --]
[#-- @ftlvariable name="stats" type="bamboo.crawl.CrawlStats" --]
[@page title="Crawl: ${crawl.name}"]

[#macro indexingProgress type todo]
    [#if todo > 0]
        ${todo} warc files require ${type} indexing
        [@progress now=(crawl.warcFiles - todo) max=crawl.warcFiles /]
    [#else]
        ${type} indexing complete
    [/#if]
[/#macro]

<h3>${crawl.name} <a class="btn btn-default pull-right" href="crawls/${crawl.id?c}/edit">Edit</a></h3>

[#if crawl.state == 1]
    <div class="alert alert-info" role="alert">
        This crawl is currently being imported and the statistics below may be incomplete.
    </div>
[/#if]

[#if corruptWarcs > 0]
    <div class="alert alert-warning" role="alert">
        <strong>Possible WARC corruption:</strong> <a href="crawls/${crawl.id?c}/warcs/corrupt">${corruptWarcs} WARC files</a>  were unreadable when indexing this crawl.
    </div>
[/#if]

[#if warcsToBeCdxIndexed > 0]
    [@indexingProgress "CDX" warcsToBeCdxIndexed /]
[/#if]


<div class="row">
    <div class="col-md-8">
        <div class="description well">
            <div class="panel-body">
                ${(descriptionHtml!"No description.")?no_esc}
            </div>
        </div>
    </div>
    <div class="col-md-4">
        <table class="table">
            <colgroup>
                <col class="col-md-2">
            </colgroup>
            <tr><th>Collected</th>
                <td>${crawl.startTime!"unknown"}&ndash;<br>${crawl.endTime!"unknown"}</td>
            </tr>
            [#if pandasInstance??]
                <tr>
                    <th>PANDORA</th>
                    <td>nla.arc-${pandasInstance.pi?c}-${pandasInstance.date} ${pandasInstance.titleName}</td>
                </tr>
            [/#if]
            <tr><th>Series</th><td><a href="series/${series.id?c}">${series.name}</a></td></tr>
            <tr>
                <th>Size</th>
                <td>
                    ${crawl.records} records<br>
                    <a href="crawls/${crawl.id?c}/warcs">${crawl.warcFiles} warc files</a>; <abbr title="${crawl.warcSize} bytes">${crawl.warcDisplaySize}</abbr><br>
                    [#if stats.artifactCount > 0]
                        <a href="crawls/${crawl.id?c}/artifacts">${stats.artifactCount} artifacts</a>; <abbr title="${stats.artifactBytes} bytes">${stats.artifactDisplaySize}</abbr><br>
                    [/#if]
                </td></tr>
            <tr>
        </table>
    </div>
</div>


[/@page]
