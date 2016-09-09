[@page title="Heritrix Jobs"]

<table class="table">
    <colgroup>

    </colgroup>
    [#list jobs as job]
        <tr>
            <td><a href="${heritrixUrl}/engine/job/${job.name}">${job.name}</a></td>
            <td>
                <form method="post" action="jobs/${job.name}/delete">
                    <input type="hidden" name="csrfToken" value="${csrfToken}">
                    <button class="btn btn-danger" type="submit" onclick='return confirm("Permanently delete ${job.name}? This cannot be undone.")'>Delete</button>
                </form>
            </td>
        </tr>
    [/#list]
</table>

[/@page]