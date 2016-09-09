[@page title="${seedlist.name}"]

<h3>${seedlist.name}
    <span class="pull-right">
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
        <div class="btn-group">
            <a href="seedlists/${seedlist.id?c}/compare" class="btn btn-default">Compare</a>
            <button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                <span class="caret"></span>
                <span class="sr-only">Toggle Dropdown</span>
            </button>
            <ul class="dropdown-menu" role="menu">
                <li><a href="seedlists/${seedlist.id?c}/compare">with another seedlist</a></li>
                <li><a href="seedlists/${seedlist.id?c}/compare/pandas">with PANDAS</a></li>
            </ul>
        </div>
        <a href="seedlists/${seedlist.id?c}/edit" class="btn btn-default">Edit</a>
    </span>
</h3>

<div class="description well">
    <div class="panel-body">
        [#noescape]${descriptionHtml!"No description."}[/#noescape]
    </div>
</div>

<table class="">
    [#assign prevDomain = ""]
    [#list seeds as seed]
        [#assign domain = seed.topPrivateDomain()]
        <tr [#if domain != prevDomain]class="table-sep"[/#if]>
            <td><a href="${seed.url}">[#noescape]${seed.highlighted()}[/#noescape]</a></td>
        </tr>
        [#assign prevDomain = domain]
    [/#list]
</table>

[/@page]