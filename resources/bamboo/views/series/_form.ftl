
<div class="form-group">
    <label for="name" class="col-lg-2 control-label">Name</label>
    <div class="col-lg-10">
        <input class="form-control" id="name" value="${(series.name)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <label for="path" class="col-lg-2 control-label">Archival Path</label>
    <div class="col-lg-10">
        <input class="form-control" id="path" value="${(series.path)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <label for="description" class="col-lg-2 control-label">Description</label>
    <div class="col-lg-10">
        <textarea class="form-control" rows="10" id="description" name="description">${(crawl.description)!}</textarea>
        <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</span>
    </div>
</div>
