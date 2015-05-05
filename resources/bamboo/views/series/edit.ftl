[@page title="Edit Crawl Series"]

<ol class="breadcrumb">
    <li><a href="series">Series</a></li>
    <li><a href="series/${series.id?c}">${series.name}</a></li>
    <li class="active">Edit</li>
</ol>

<h3>Edit Series</h3>

<div class="well">
    <form method="post" class="form-horizontal">
        <input name="csrfToken" value="${csrfToken}" type="hidden">
        <fieldset>
            [#include "_form.ftl"]

            <div class="form-group">
                <label class="col-lg-2 control-label">Collections</label>
                <div class="col-lg-10">
                    <table class="table" id="collectionsTable">
                        <colgroup>
                            <col class="col-md-2">
                        </colgroup>
                        <tbody>
                            <tr>
                                <th>Name</th>
                                <th>URL Filters</th>
                            </tr>
                            [#if collections?size > 0]
                                [#list collections as collection]
                                    <tr id="collection-${collection.id?c}">
                                        <td>${collection.name}<input type='hidden' name='collection.id' value="${collection.id?c}"></td>
                                        <td><textarea name="collection.urlFilters">${collection.urlFilters}</textarea></td>
                                        <td><button class='btn btn-danger removeButton' type='button'>Remove</button></td>
                                    </tr>
                                [/#list]
                            [#else]
                                <tr><td colspan="3">None</td></tr>
                            [/#if]
                            <tr>
                                <td colspan="2">
                                    <div class="row">
                                        <div class="col-md-8">
                                            <select class="form-control" id="addToCollectionDropDown">
                                                [#list allCollections as collection]
                                                <option value="${collection.id?c}">${collection.name}</option>
                                                [/#list]
                                            </select>
                                        </div>
                                        <div class="col-md-2">
                                            <button class="btn btn-sm btn-default" type="button" id="addToCollectionButton">Add</button>
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Save Changes</button>
                    <a href="series/${series.id?c}" class="btn btn-default">Cancel</a>
                </div>
            </div>

        </fieldset>
    </form>
</div>

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