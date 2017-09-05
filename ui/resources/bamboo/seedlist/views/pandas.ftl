[@page title="Comparing ${seedlist.name} to PANDAS"]

<h3>Comparing ${seedlist.name} to PANDAS</h3>

<p>Found ${matches.size()} matching titles in the PANDAS database.</p>

<table class="table table-no-sep">
    <colgroup>
        <col width="64px" />
        <col />
        <col />
        <col />
        <col />
        <col width="180px" />
    </colgroup>
    <thead>
        <tr>
            <td></td>
            <td>PI</td>
            <td>Title</td>
            <td>Agency</td>
            <td>Owner</td>
            <td>Status</td>
        </tr>
    </thead>
    <tbody>
        [#assign prevDomain = ""]
        [#assign prevUrl = ""]
        [#list matches as match]
            [#assign seed = match.seed]
            [#assign title = match.title]
            [#assign domain = seed.topPrivateDomain()]
            [#if seed.url != prevUrl]
                <tr [#if domain != prevDomain]class="table-sep"[/#if]><td colspan="6"><a href="${seed.url}">${seed.highlighted()?no_esc}</a></td></tr>
            [/#if]
            <tr>
                <td></td>
                <td>${title.pi?c}</td>
                <td>${title.name}</td>
                <td>${title.agency}</td>
                <td>${title.owner!}</td>
                <td><span class="[#switch title.status]
                [#case 'rejected'][#case 'ceased'][#case 'permission impossible'][#case 'permission denied']status-rejected[#break]
                [#case 'permission granted']status-granted[#break]
                [#default]status-default[#break]
                [/#switch]">${title.status}</span></td>
            </tr>
            [#assign prevDomain = domain]
            [#assign prevUrl = seed.url]
        [/#list]
    </tbody>
</table>

<h3>Unmatched Seeds</h3>
<p>Matching PANDAS titles were not found for ${unmatched.size()} seeds.</p>

<table class="">
    [#assign prevDomain = ""]
    [#list unmatched as seed]
    [#assign domain = seed.topPrivateDomain()]
    <tr [#if domain != prevDomain]class="table-sep"[/#if]>
        <td><a href="${seed.url}">${seed.highlighted()?no_esc}</a></td>
    </tr>
    [#assign prevDomain = domain]
    [/#list]
</table>

[/@page]