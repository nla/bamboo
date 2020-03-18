[@page title="New Crawl"]

    <h3>New Crawl</h3>
    <div class="well">
        <form method="post" class="form-vertical" enctype="multipart/form-data">
            <fieldset>
                <div class="form-group">
                    <label for="name" class="col-lg-2 control-label">Name</label>
                    <div class="col-lg-10">
                        <input class="form-control" id="name" name="name" required>
                    </div>
                </div>


                <div class="form-group">
                    <label for="description" class="col-lg-2 control-label">Description</label>
                    <div class="col-lg-10">
                        <textarea class="form-control" rows="10" id="description" name="description"></textarea>
                        <span class="help-block"><a href="https://help.github.com/articles/markdown-basics/">Markdown</a> is enabled.</span>
                    </div>
                </div>

                <!--
                <div class="form-group">
                    <label for="crawlSeriesId" class="col-lg-2 control-label">Crawl Series</label>
                    <div class="col-lg-10">
                        <select id="crawlSeriesId" name="crawlSeriesId" class="form-control" required>
                            <option selected disabled hidden></option>
                            [#list allCrawlSeries as series]
                                <option value="${series.id?c}"[#if series.id == selectedCrawlSeriesId!-1] selected[/#if]>${series.name}</option>
                            [/#list]
                        </select>
                    </div>
                </div>
                -->

                <div class="form-group">
                    <label for="warcFile" class="col-lg-2 control-label">WARC Files</label>
                    <input id="warcFile" type="file" name="warcFile" multiple accept="application/warc,.warc.gz,.warc,.arc.gz,.arc">
                </div>

                <div class="form-group">
                    <label for="artifact" class="col-lg-2 control-label">Other Crawl Artifacts</label>
                    <input id="artifact" type="file" name="artifact" multiple>
                </div>

                <div class="form-group">
                    <div class="col-lg-10 col-lg-offset-2">
                        <button type="submit" class="btn btn-primary">Save Changes</button>
                        [#if (selectedCrawlSeriesId!-1) != -1]
                            <a href="series/${selectedCrawlSeriesId?c}" class="btn btn-default">Cancel</a>
                        [#else]
                            <a href="crawls" class="btn btn-default">Cancel</a>
                        [/#if]
                    </div>
                </div>
            </fieldset>
        </form>
    </div>
[/@page]