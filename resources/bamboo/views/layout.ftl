[#macro page title]
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <base href="${request.contextPath()}">
    <title>${title} - Bamboo</title>
    <link rel="stylesheet" href="webjars/bootstrap/3.3.1/css/bootstrap.min.css">
    <!--<link rel="stylesheet" href="webjars/bootstrap/3.3.1/css/bootstrap-theme.min.css"> -->
    <link rel="stylesheet" href="assets/bamboo.css">
</head>
<body>
<nav class="navbar navbar-inverse navbar-fixed-top">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="#">BambooWeb</a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
            <ul class="nav navbar-nav navbar-right">
                <li><a href="http://pandas.nla.gov.au/">PANDAS</a></li>
                <li><a href="#">Settings</a></li>
                <li><a href="#">Profile</a></li>
                <li><a href="#">Help</a></li>
            </ul>
            <form class="navbar-form navbar-right">
                <input type="text" class="form-control" placeholder="Search...">
            </form>
        </div>
    </div>
</nav>

<div class="container-fluid">
    <div class="row">
        <div class="col-sm-3 col-md-2 sidebar">
            <ul class="nav nav-sidebar">
                <li><a href="#">Overview</a></li>
                <li><a href="#">Collections</a></li>
                <li><a href="series">Crawl Series</a></li>
                <li><a href="crawls">Crawls</a></li>
            </ul>
            <ul class="nav nav-sidebar">
                <li><a href="import">Import Crawl from Heritrix</a></li>
            </ul>

            <ul class="nav nav-sidebar">
                <li><a href="http://dl.nla.gov.au/agwa/stayback/">Access Control</a></li>
            </ul>
        </div>
        <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
            <h1 class="page-header">${title}</h1>
    [#nested/]
        </div>
    </div>
</div>
</body>
</html>
[/#macro]

[#--
 # Format Number of Bytes in SI Units
 # from Horatio Alderaan's https://stackoverflow.com/a/20622900
 # --]
[#function si num]
    [#assign order     = num?round?c?length /]
    [#assign thousands = ((order - 1) / 3)?floor /]
    [#if (thousands < 0)][#assign thousands = 0 /][/#if]
    [#assign siMap = [ {"factor": 1, "unit": ""}, {"factor": 1000, "unit": "K"}, {"factor": 1000000, "unit": "M"}, {"factor": 1000000000, "unit":"G"}, {"factor": 1000000000000, "unit": "T"} ]/]
    [#assign siStr = (num / (siMap[thousands].factor))?string("0.#") + siMap[thousands].unit /]
    [#return siStr /]
[/#function]

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
    [#assign url = request.contextUri().relativize(request.uri()).getPath()]
    [#if current < 5]
        [#assign pages = 1..min(last, 5)]
    [#elseif last - current < 5]
        [#assign pages = max(last - 5, 1)..last]
    [#else]
        [#assign pages = (current - 2)..(current + 2)]
    [/#if]
    <nav>
        <ul class="pagination">
            [#if (current > 1)]
                <li><a href="${url}?page=${current - 1}" aria-label="Previous">
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
                    <li><a href="${url}?page=${page}">${page}</a></li>
                [/#if]
            [/#list]
            [#if (last - current >= 5)]
                <li class="disabled"><span aria-hidden="true">&hellip;</span></li>
                <li><a href="${url}?page=${last}">${last}</a></li>
            [/#if]
            [#if current < last]
                <li><a href="${url}?page=${current + 1}" aria-label="Next">
                    <span aria-hidden="true">&raquo;</span>
                </a></li>
            [#else]
                <li class="disabled"><span aria-hidden="true">&raquo;</span></li>
            [/#if]
        </ul>
    </nav>
[/#macro]

[#macro progress now max=100 min=0]
<div class="progress">
    <div class="progress-bar progress-bar-striped" role="progressbar" aria-valuenow="${now}" aria-valuemin="0" aria-valuemax="${max}" style="width: ${100 * now / max}%">
        ${now} / ${max}
        <span class="sr-only">${100 * now / max}% Complete</span>

    </div>
</div>
[/#macro]