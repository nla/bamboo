[@page title="Edit Category"]


<h3>Edit Category</h3>

<div class="well">
    <form method="post" class="form-horizontal">
        <fieldset>
            [#include "_form.ftl"]

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Save Changes</button>
s                    <a href="directory/category/${category.id?c}" class="btn btn-default">Cancel</a>
                </div>
            </div>

        </fieldset>
    </form>
</div>

[/@page]