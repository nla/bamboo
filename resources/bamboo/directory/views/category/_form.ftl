<div class="form-group">
    <label for="name" class="col-lg-2 control-label">Name</label>
    <div class="col-lg-10">
        <input class="form-control" id="name" value="${(category.name)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <label for="parent_id" class="col-lg-2 control-label">Parent</label>
    <div class="col-lg-10">
        <select class="form-control" name="parentId">
            <option selected="selected"></option>
        </select>
    </div>
</div>

<div class="form-group">
    <label for="description" class="col-lg-2 control-label">Description</label>
    <div class="col-lg-10">
        <textarea class="form-control" rows="4" id="description" name="description">${(category.description)!}</textarea>
        <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</tt></span>
    </div>
</div>
