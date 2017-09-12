
<div class="form-group">
    <label for="name" class="col-lg-2 control-label">Name</label>
    <div class="col-lg-10">
        <input class="form-control" id="name" value="${(series.name)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <div class="col-lg-2"></div>
    <div class="col-lg-10 checkbox">
        <label>
            <input type="checkbox" name="pandora" [#if (series.pandora)!false]checked[/#if]>
            Treat as PANDORA content (for Trove search purposes)
        </label>
    </div>
</div>


<div class="form-group">
    <label for="path" class="col-lg-2 control-label">Archival Path</label>
    <div class="col-lg-10">
        <input class="form-control" id="path" value="${(series.path)!}" name="path">
    </div>
</div>

<div class="form-group">
    <label for="description" class="col-lg-2 control-label">Description</label>
    <div class="col-lg-10">
        <textarea class="form-control" rows="10" id="description" name="description">${(series.description)!}</textarea>
        <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</span>
    </div>
</div>
