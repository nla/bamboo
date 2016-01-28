[@page title="Edit Crawl"]

<h3>Edit Crawl</h3>
<div class="well">
<form method="post" class="form-vertical">
    <input name="csrfToken" value="${csrfToken}" type="hidden">

    <fieldset>
        <div class="form-group">
            <label for="name" class="col-lg-2 control-label">Name</label>
            <div class="col-lg-10">
                <input class="form-control" id="name" value="${(crawl.name)!}" name="name" required>
            </div>
        </div>


        <div class="form-group">
            <label for="description" class="col-lg-2 control-label">Description</label>
            <div class="col-lg-10">
                <textarea class="form-control" rows="10" id="description" name="description">${(crawl.description)!}</textarea>
                <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</span>
            </div>
        </div>

        <div class="form-group">
            <div class="col-lg-10 col-lg-offset-2">
                <button type="submit" class="btn btn-primary">Save Changes</button>
                <a href="crawls/${crawl.id?c}" class="btn btn-default">Cancel</a>
            </div>
        </div>
    </fieldset>
</form>
</div>
[/@page]