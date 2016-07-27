[@page title="Directory"]

<h3>Web Directory</h3>

<ul>
  [#list categories as category]
    <li><a href="directory/category/${category.id?c}">${category.name}</a></li>
  [/#list]
</ul>

<a href="directory/category/new" class="btn btn-primary">New Category</a>

[/@page]