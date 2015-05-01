[@page title="New Series"]

<ol class="breadcrumb">
    <li><a href="series">Series</a></li>
    <li class="active">New</li>
</ol>

<h3>New Series</h3>

<div class="well">
    <form method="post" class="form-horizontal">
        <input name="csrfToken" value="${csrfToken}" type="hidden">

        <fieldset>
            [#include "_form.ftl"]

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Create Series</button>
                    <a href="series" class="btn btn-default">Cancel</a>
                </div>
            </div>
        </fieldset>
    </form>
</div>

[/@page]