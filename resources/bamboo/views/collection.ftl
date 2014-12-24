[@page title="${collection.name}"]

<h3>CDXs</h3>
<ul>
    [#list cdxs as cdx]
    <li><a href="cdx/${cdx.id}">${cdx.path}</a></li>
    [/#list]
</ul>
[/@page]