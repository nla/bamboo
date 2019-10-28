[@page title="${category.name}"]

<h3>${category.name} <a class="btn btn-default pull-right" href="directory/category/${category.id?c}/edit">Edit</a></h3>

<h4>Subcategories
    <div class="pull-right">
        <a href="directory/category/${category.id?c}/link" class="btn btn-default">Link Category</a>
        <a href="directory/category/new?parentId=${category.id?c}" class="btn btn-primary">New Subcategory</a>
    </div>
</h4>

[#if subcategories?has_content]
    <p>No subcategories.</p>
[#else]
    <table class="table">
        <thead>
        <tr>
            <td>Name</td>
        </tr>
        </thead>
        <tbody>
            [#list subcategories as subcategory]
            <tr>
                <td><a href="directory/category/${subcategory.id?c}">${subcategory.name}</a></td>
            </tr>
            [/#list]
        </tbody>
    </table>
[/#if]


<h4>Subcategories
    <div class="pull-right">
        <a href="directory/category/${category.id?c}/link" class="btn btn-default">Link Category</a>
        <a href="directory/category/new?parentId=${category.id?c}" class="btn btn-primary">New Subcategory</a>
    </div>
</h4>

[#if sites?has_content]
    <p>No sites found.</p>
    [#else]
    <table class="table">
        <thead>
        <tr>
            <td>Name</td>
        </tr>
        </thead>
        <tbody>
            [#list sites as site]
            <tr>
                <td><a href="directory/sites/${site.id?c}">${site.name}</a></td>
            </tr>
            [/#list]
        </tbody>
    </table>
[/#if]

[/@page]