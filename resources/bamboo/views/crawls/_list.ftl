<table class="table">
    [#list crawls as crawl]
    <tr>
        <td><a href="crawls/${crawl.id}">${crawl.name}</a></td>
    </tr>
    [/#list]
</table>