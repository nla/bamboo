[@page title="${crawl.name}"]
<table class="table">
    <tr><th>State</th><td>${crawl.stateName()}</td></tr>
</table>
<form method="post" action="crawls/${crawl.id}/buildcdx">
    <button type="submit">Build CDX Index</button>
</form>
[/@page]