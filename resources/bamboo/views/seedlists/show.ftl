[@page title="${seedlist.name}"]

<h3>${seedlist.name}
    <span class="pull-right">
        <a href="seedlists/${seedlist.id?c}/import" class="btn btn-default">Add Seeds</a>
        <div class="btn-group">
            <a href="seedlists/${seedlist.id?c}/export/urls" class="btn btn-default">Export</a>
            <button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-expanded="false">
                <span class="caret"></span>
                <span class="sr-only">Toggle Dropdown</span>
            </button>
            <ul class="dropdown-menu" role="menu">
                <li><a href="seedlists/${seedlist.id?c}/export/urls">Plain text (URLs)</a></li>
                <li><a href="seedlists/${seedlist.id?c}/export/surts">Plain text (SURTs)</a></li>
            </ul>
        </div>
        <a href="seedlists/${seedlist.id?c}/edit" class="btn btn-default">Edit</a>
    </span>
</h3>

<table class="">
    [#assign prevDomain = ""]
    [#list seeds as seed]
        [#assign domain = seed.topPrivateDomain()]
        <tr [#if domain != prevDomain]class="table-sep"[/#if]>
            <td>[#noescape]${seed.highlighted()}[/#noescape]</td>
        </tr>
        [#assign prevDomain = domain]
    [/#list]
</table>

[/@page]