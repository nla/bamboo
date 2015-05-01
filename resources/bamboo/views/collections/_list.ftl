<table class="table">
    <thead>
        <tr>
            <td>Name</td>
        </tr>
    </thead>
    <tbody>
        [#list collections as collection]
            <tr>
                <td><a href="collections/${collection.id}">${collection.name}</a></td>
            </tr>
        [/#list]
    </tbody>
</table>