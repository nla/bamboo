[@page title="New Seedlist"]

<h3>New Seedlist</h3>

<div class="well">
    <form method="post" action="seedlists/create" class="form-horizontal">
        <fieldset>
            [#include "_form.ftlh"]

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Create Seedlist</button>
                    <a href="seedlists" class="btn btn-default">Cancel</a>
                </div>
            </div>
        </fieldset>
    </form>
</div>

[/@page]