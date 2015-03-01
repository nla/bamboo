[@page title="Edit Collection"]
<form method="post">
    <input name="csrfToken" value="${csrfToken}" type="hidden">
    [#include "_form.ftl"]
    <button type="submit" class="btn btn-primary">Save</button>
</form>
[/@page]