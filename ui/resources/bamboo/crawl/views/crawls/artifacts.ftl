[#-- @ftlvariable name="artifacts" type="java.util.List<bamboo.crawl.Artifact>" --]
[@page title="Crawl artifacts for ${crawl.name}"]

<h3>Crawl artifacts for <a href="crawls/${crawl.id?c}">${crawl.name}</a>
</h3>

<table class="table">
    <thead>
        <tr>
            <td>Filename</td>
            <td>Type</td>
            <td>SHA256</td>
            <td>Size</td>
            <td></td>
        </tr>
    </thead>
    <tbody>
    [#list artifacts as artifact]
        <tr>
            <td><a href="crawls/${crawl.id?c}/artifacts/${artifact.relpath}">${artifact.relpath}</a></td>
            <td>${artifact.type}</td>
            <td><abbr title="${artifact.sha256}">${artifact.sha256?substring(0, 8)}...</abbr></td>
            <td>${si(artifact.size)}</td>
        </tr>
    [/#list]
    </tbody>
</table>

[/@page]