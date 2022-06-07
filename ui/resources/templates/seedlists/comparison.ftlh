[#macro seedtable seeds]
    <table class="">
        [#assign prevDomain = ""]
        [#list seeds as seed]
        [#assign domain = seed.topPrivateDomain()]
        <tr [#if domain != prevDomain]class="table-sep"[/#if]>
            <td><a href="${seed.url}">${seed.highlighted()?no_esc}</a></td>
        </tr>
        [#assign prevDomain = domain]
        [/#list]
    </table>
[/#macro]

[#macro seedpanel seeds baseUrl]
    <div class="panel panel-default">
        <div class="panel-heading">
            <h3 class="panel-title pull-left">
                [#nested/]
            </h3>

            <div class="pull-right">
                <div class="btn-group">
                    <a href="${baseUrl}/saveas" class="btn btn-default">Save As</a>
                </div>

                <div class="btn-group">
                    <a href="${baseUrl}/export/urls" class="btn btn-default">Export</a>
                    <button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <span class="caret"></span>
                        <span class="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul class="dropdown-menu" role="menu">
                        <li><a href="${baseUrl}/export/urls">Plain text (URLs)</a></li>
                        <li><a href="${baseUrl}/export/surts">Plain text (SURTs)</a></li>
                    </ul>
                </div>
            </div>

            <div class="clearfix"></div>
        </div>
        <div class="panel-body">
            [@seedtable seeds /]
        </div>
    </div>
[/#macro]

[@page title="Comparing ${seedlist1.name} to ${seedlist2.name}"]
    [#assign baseUrl = "seedlists/${seedlist1.id?c}/compare/${seedlist2.id?c}" /]

    <h3>Comparing <a href="seedlists/${seedlist1.id?c}">${seedlist1.name}</a> to <a href="seedlists/${seedlist2.id?c}">${seedlist2.name}</a></h3>

    [@seedpanel onlyIn1 "${baseUrl}/onlyin1"]
        ${onlyIn1?size} seeds unique to <a href="seedlists/${seedlist1.id?c}">${seedlist1.name}</a>
    [/@seedpanel]

    [@seedpanel onlyIn2 "${baseUrl}/onlyin2"]
        ${onlyIn2?size} seeds unique to <a href="seedlists/${seedlist2.id?c}">${seedlist2.name}</a>
    [/@seedpanel]

    [@seedpanel common "${baseUrl}/common"]
        ${common?size} seeds in common
    [/@seedpanel]

[/@page]