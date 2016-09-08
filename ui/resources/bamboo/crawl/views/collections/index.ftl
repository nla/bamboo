[@page title="Collections"]

<h3>All Collections <a href="collections/new" class="btn btn-primary pull-right">New Collection</a></h3>

[#if collections.isEmpty()]
    <p>No collections found.</p>
[#else]
    [#include "/collections/_list.ftl"]
    [@pagination collectionsPager.currentPage collectionsPager.lastPage /]
[/#if]

[/@page]