<table class="table">
    <thead>
        <tr>
            <td>Name</td>
            <td>Size</td>
        </tr>
    </thead>
    <tbody>
        [#list collections as collection]
            <tr>
                <td><a href="collections/${collection.id?c}">${collection.name}</a></td>
                <td>${collection.recordBytesDisplay}</td>
            </tr>
        [/#list]
    </tbody>
</table>