[#macro page title]
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <base href="${requestContext.contextPath!""}/">
    <title>${title} - Bamboo</title>
    <link rel="stylesheet" href="webjars/bootswatch-paper/3.3.4%2B1/css/bootstrap.min.css">
    <link rel="stylesheet" href="assets/bamboo.css">
</head>
<body>

<nav class="navbar navbar-default navbar-static-top">
    <div class="container">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
            <ul class="nav navbar-nav nav-tabs">
                <li [#if requestContext.requestUri = "/"]class="active"[/#if]><a class="navbar-brand" href=".">Bamboo</a>
                [#if auth.hasRole("panadmin")]
                    <li [#if requestContext.requestUri?starts_with("/crawls")]class="active"[/#if]><a href="crawls">Crawls</a>
                [/#if]
                <li [#if requestContext.requestUri?starts_with("/series")]class="active"[/#if]><a href="series" id="navCrawlSeries">Series</a>
                [#if auth.hasRole("panadmin")]
                    <li [#if requestContext.requestUri?starts_with("/collections")]class="active"[/#if]><a href="collections">Collections</a>
                    <li [#if requestContext.requestUri?starts_with("/seedlists")]class="active"[/#if]><a href="seedlists">Seedlists</a>
                [/#if]
            </ul>

            <ul class="nav navbar-nav navbar-right nav-tabs">
                <!--
                <li>
                    <form class="navbar-form navbar-right">
                        <input type="text" class="form-control" placeholder="Search...">
                    </form>
                </li>
                -->
                [#if auth.hasRole("panadmin")]
                    <li><a href="http://pandas.nla.gov.au/cdx/">Access Control</a></li>
                    <li [#if requestContext.requestUri?starts_with("/tasks")]class="active"[/#if]><a href="tasks">Task Manager</a></li>
                [/#if]
                [#if auth.authenticated]
                    <li class="dropdown">
                        <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">${auth.username} <span class="caret"></span></a>
                        <ul class="dropdown-menu">
                            <li><a href="logout">Sign Out</a></li>
                        </ul>
                    </li>
                [#else]
                    <li><a href="login">Sign In</a></li>
                [/#if]
            </ul>
        </div>
    </div>
</nav>

<div class="container main">
    [#nested/]
</div>
<script src="webjars/jquery/2.1.3/jquery.min.js"></script>
<script src="webjars/bootstrap/3.3.4/js/bootstrap.min.js"></script>
</body>
</html>
[/#macro]

[#function max x y]
    [#if x > y]
        [#return x]
    [#else]
        [#return y]
    [/#if]
[/#function]

[#function min x y]
    [#if x < y]
        [#return x]
    [#else]
        [#return y]
    [/#if]
[/#function]

[#macro pagination current last]
    [#assign url = requestContext.requestUri]
    [#if current < 5]
        [#assign pages = 1..min(last, 5)]
    [#elseif last - current < 5]
        [#assign pages = max(last - 5, 1)..last]
    [#else]
        [#assign pages = (current - 2)..(current + 2)]
    [/#if]
<div class="row">
    <div class="col-md-12 text-center">
    <nav>
        <ul class="pagination">
            [#if (current > 1)]
                <li><a href="${url}?page=${(current - 1)?c}" aria-label="Previous">
                    <span aria-hidden="true">&laquo;</span>
                </a></li>
            [#else]
                <li class="disabled"><span aria-hidden="true">&laquo;</span></li>
            [/#if]
            [#if (current >= 5)]
                <li><a href="${url}?page=1">1</a></li>
                <li class="disabled"><span aria-hidden="true">&hellip;</span></li>
            [/#if]
            [#list pages as page]
                [#if page == current]
                    <li class="active"><span>${page}</span></li>
                [#else]
                    <li><a href="${url}?page=${page?c}">${page}</a></li>
                [/#if]
            [/#list]
            [#if (last - current >= 5)]
                <li class="disabled"><span aria-hidden="true">&hellip;</span></li>
                <li><a href="${url}?page=${last?c}">${last}</a></li>
            [/#if]
            [#if current < last]
                <li><a href="${url}?page=${(current + 1)?c}" aria-label="Next">
                    <span aria-hidden="true">&raquo;</span>
                </a></li>
            [#else]
                <li class="disabled"><span aria-hidden="true">&raquo;</span></li>
            [/#if]
        </ul>
    </nav>
        </div></div>

[/#macro]

[#macro progress now max=100 min=0]
<div class="progress">
    <div class="progress-bar progress-bar-striped" role="progressbar" aria-valuenow="${now}" aria-valuemin="0" aria-valuemax="${max}" style="width: ${100 * now / max}%">
        ${now} / ${max}
        <span class="sr-only">${100 * now / max}% Complete</span>

    </div>
</div>
[/#macro]