[#macro seedtable seeds]
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
[/#macro]

[@page title="Comparing ${seedlist1.name} to ${seedlist2.name}"]

    <h3>Comparing <a href="seedlists/${seedlist1.id?c}">${seedlist1.name}</a> to <a href="seedlists/${seedlist2.id?c}">${seedlist2.name}</a></h3>

    <h4>${onlyIn1.size()} seeds are only in <a href="seedlists/${seedlist1.id?c}">${seedlist1.name}</a></h4>

    [@seedtable onlyIn1 /]

    <h4>${onlyIn2.size()} seeds are only in <a href="seedlists/${seedlist2.id?c}">${seedlist2.name}</a></h4>

    [@seedtable onlyIn2 /]

    <h4>${common.size()} seeds are in both seedlists</h4>

    [@seedtable common /]

[/@page]