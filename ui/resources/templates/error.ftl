[@page title="Error"]

    <h3>${status} ${error}</h3>

    [#if status = 404]
        <p>Alas! There's nothing here.</p>
    [#else]
        <p>Yikes! That's an error.</p>
        <p>If you report it please include the following diagnostic information.</p>

        <pre>${timestamp?datetime?iso_local} ${path!""} ${status!""} ${error!""}
${trace!""}</pre>
    [/#if]
[/@page]