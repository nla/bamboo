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
                <li class="active"><a href="#">Overview <span class="sr-only">(current)</span></a></li>
                <li><a href="#">Collections (TODO)</a></li>
                <li><a href="#">Crawls (TODO)</a></li>
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