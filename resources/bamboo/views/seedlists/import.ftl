[@page title="Adding new seeds to ${seedlist.name}"]

<h3>Adding new seeds to ${seedlist.name}</h3>

<div class="well">
    <form method="post" class="form-horizontal">
        <input name="csrfToken" value="${csrfToken}" type="hidden">

        <fieldset>
            <div class="form-group">
                <label for="urls" class="col-lg-2 control-label">URLs</label>
                <div class="col-lg-10">
                    <textarea class="form-control" id="urls" name="urls" rows="20" required></textarea>
                </div>
            </div>

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Add Seeds</button>
                    <a href="seedlists/${seedlist.id?c}" class="btn btn-default">Cancel</a>
                </div>
            </div>
        </fieldset>
    </form>
</div>
[/@page]