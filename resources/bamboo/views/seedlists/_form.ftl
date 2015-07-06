<div class="form-group">
    <label for="name" class="col-lg-2 control-label">Name</label>
    <div class="col-lg-10">
        <input class="form-control" id="name" value="${(seedlist.name)!}" name="name" required>
    </div>
</div>

<div class="form-group">
    <label for="seeds" class="col-lg-2 control-label">Seeds</label>
    <div class="col-lg-10">
        <textarea name="seeds" id="seeds" class="form-control" rows="30">[#if seeds??][#list seeds as seed]${seed.url}
[/#list][/#if]</textarea>
    </div>
</div>
