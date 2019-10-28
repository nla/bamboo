[@page title="Edit Seedlist"]

<h3>Edit Seedlist</h3>

<div class="well">
    <form method="post" class="form-horizontal">
        <fieldset>
            [#include "_form.ftl"]

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Save Changes</button>
                    <a href="seedlists/${seedlist.id?c}" class="btn btn-default">Cancel</a>
                    <button formaction="seedlists/${seedlist.id?c}/delete" type="submit" class="btn btn-danger pull-right">Delete Seedlist</button>
                </div>
            </div>
        </fieldset>
    </form>
</div>
[/@page]