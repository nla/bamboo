[@page title="Upload into ${crawl.name}"]

    <h3>Upload into <a href="crawls/${crawl.id?c}">${crawl.name}</a></h3>

    <form method="post" enctype="multipart/form-data">
        <fieldset>
            <div class="form-group">
                <label for="warcFile" class="col-lg-2 control-label">WARC Files</label>
                <input id="warcFile" type="file" name="warcFile" multiple accept="application/warc,.warc.gz,.warc,.arc.gz,.arc,.gz">
            </div>

            <div class="form-group">
                <div class="col-lg-10 col-lg-offset-2">
                    <button type="submit" class="btn btn-primary">Upload</button>
                    <a href="crawls/${crawl.id?c}/warcs" class="btn btn-default">Cancel</a>
                </div>
            </div>
        </fieldset>
    </form>

[/@page]