<div class="form-group">
    <label for="name" class="col-lg-2 control-label">Name</label>
    <div class="col-lg-10">
        <input class="form-control" id="name" value="${(collection.name)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <label for="description" class="col-lg-2 control-label">Description</label>
    <div class="col-lg-10">
        <textarea class="form-control" rows="10" id="description" name="description">${(collection.description)!}</textarea>
        <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</tt></span>
    </div>
</div>


<div class="form-group">
    <label for="cdxUrl" class="col-lg-2 control-label">CDX Url</label>
    <div class="col-lg-10">
        <input class="form-control" id="cdxUrl" value="${(collection.cdxUrl)!}" name="cdxUrl" type="url">
    </div>
</div>

