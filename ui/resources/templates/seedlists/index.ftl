[@page title="Seedlists"]

<h3>All Seedlists <a href="seedlists/new" class="btn btn-primary pull-right">New Seed List</a></h3>

[#if seedlists?has_content]
    <p>No seedlists found.</p>
[#else]
    <table class="table">
        <thead>
        <tr>
            <td>Name</td>
            <td>Seeds</td>
        </tr>
        </thead>
        <tbody>
        [#list seedlists as seedlist]
            <tr>
                <td><a href="seedlists/${seedlist.id?c}">${seedlist.name}</a></td>
                <td>${seedlist.totalSeeds}</td>
            </tr>
        [/#list]
        </tbody>
    </table>
[/#if]

[/@page]