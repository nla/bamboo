[#if parent??]
    [#assign noun='Subcategory']
[#else]
    [#assign noun='Category']
[/#if]

[@page title="New ${noun}"]

<h3>New ${noun}</h3>

<div class="well">
    <form method="post" action="directory/category/create" class="form-horizontal">
        <input name="csrfToken" value="${csrfToken}" type="hidden">

        <fieldset>
            [#include "_form.ftl"]

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Create ${noun}</button>
                    [#if parent??]
                        <a href="directory/category/${parent.id?c}" class="btn btn-default">Cancel</a>
                    [#else]
                        <a href="directory" class="btn btn-default">Cancel</a>
                    [/#if]
                </div>
            </div>
        </fieldset>
    </form>
</div>

[/@page]