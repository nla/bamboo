[@page title="Edit Crawl Series"]
<form method="post">
    <input name="csrfToken" value="${csrfToken}" type="hidden">
    [#include "_form.ftl"]

    <h3>Collections</h3>
    <table class="table" id="collectionsTable">
        <colgroup>
            <col class="col-md-2">
        </colgroup>
        <tbody>
            <tr>
                <th>Name</th>
                <th>URL Filters</th>
            </tr>
            [#list collections as collection]
                <tr id="collection-${collection.id}">
                    <td>${collection.name}<input type='hidden' name='collection.id' value="${collection.id}"></td>
                    <td><textarea name="collection.urlFilters">${collection.urlFilters}</textarea></td>
                    <td><button class='btn btn-danger removeButton' type='button'>Remove</button></td>
                </tr>
            [/#list]
            <tr>
                <td>
                    <select class="chosen" id="addToCollectionDropDown">
                        [#list allCollections as collection]
                            <option value="${collection.id}">${collection.name}</option>
                        [/#list]
                    </select>
                    <button class="btn btn-primary" type="button" id="addToCollectionButton">Add</button>
                </td>
            </tr>
        </tbody>
    </table>

    <button type="submit" class="btn btn-primary">Save</button>
</form>

<script src="webjars/jquery/2.1.3/jquery.min.js"></script>
<script src="webjars/chosen/1.2.0/chosen.jquery.min.js"></script>
<link rel="stylesheet" href="webjars/chosen/1.2.0/chosen.min.css">
<script>
$(".chosen").chosen();

function removeFromCollection() {
    $(this).closest('tr').remove();
}

$(".removeButton").click(removeFromCollection);

$("#addToCollectionButton").click(function() {
    var dropDown = $("#addToCollectionDropDown")[0];
    var option = dropDown.options[dropDown.selectedIndex];
    var collectionId = option.value;
    var row = $("#collection-" + collectionId);
    if (row.length == 0) {
        row = $("<tr>").attr("id", "collection-" + collectionId).hide();
        $("<td>").text(option.text).append($("<input type='hidden' name='collection.id'>").attr("value", option.value)).appendTo(row);
        $("<td><textarea name='collection.urlFilters'></textarea></td>").appendTo(row);
        $("<td>").append($("<button class='btn btn-danger' type='button'>Remove</button>").click(removeFromCollection)).appendTo(row);
        $("#collectionsTable > tbody > tr:last").before(row);
    } else {
        row.fadeOut(200);
    }
    row.fadeIn();
});

</script>
[/@page]