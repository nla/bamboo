[@page title="Comparing ${seedlist.name} with another seedlist"]

<h3>Comparing <a href="seedlists/${seedlist.id?c}">${seedlist.name}</a> with another seedlist</h3>

<p>Choose a second seedlist to compare with.</p>

<table class="table">
    <thead>
    <tr>
        <td>Name</td>
        <td>Seeds</td>
    </tr>
    </thead>
    <tbody>
    [#list seedlists as other]
        [#if seedlist.id != other.id]
            <tr>
                <td><a href="seedlists/${seedlist.id?c}/compare/${other.id?c}">${other.name}</a></td>
                <td>${other.totalSeeds}</td>
            </tr>
        [/#if]
    [/#list]
    </tbody>
</table>

[/@page]