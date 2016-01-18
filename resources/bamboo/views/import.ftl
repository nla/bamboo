[@page title="Import Crawl"]

<ol class="breadcrumb">
    <li><a href="crawls">Crawls</a></li>
    <li class="active">Import</li>
</ol>

<h3>Import Crawl from Heritrix</h3>

<div class="well">
    <form method="POST" class="form-horizontal">
        <input name="csrfToken" value="${csrfToken}" type="hidden">

        <fieldset>
            <div class="form-group">
                <label for="heritrixJob" class="col-lg-2 control-label">Heritrix Job:</label>
                <div class="col-lg-10">
                    <select id="heritrixJob" name="heritrixJob" class="form-control" required>
                        <option selected disabled hidden></option>
                        [#list jobs as job]
                            <option>${job.name}</option>
                        [/#list]
                    </select>
                </div>
            </div>

            <div class="form-group">
                <label for="crawlSeriesId" class="col-lg-2 control-label">Crawl Series:</label>
                <div class="col-lg-10">
                    <select id="crawlSeriesId" name="crawlSeriesId" class="form-control" required>
                        <option selected disabled hidden></option>
                        [#list allCrawlSeries as series]
                            <option value="${series.id?c}"[#if series.id == selectedCrawlSeriesId!-1] selected[/#if]>${series.name}</option>
                        [/#list]
                    </select>
                </div>
            </div>
            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Import Crawl</button>
                    <a href="crawls" class="btn btn-default">Cancel</a>
                </div>
            </div>

        </fieldset>

    </form>
</div>

[/@page]