[@page title="Overview"]

<h3>Dashboard</h3>
<p><i>TBD: What sort of useful overview things should we include here?</i></p>
<div id="dummy-chart" style="width: 400px; height: 400px;"></div>
<p></p>

<script src="webjars/flotr2/d43f8566e8/flotr2.min.js"></script>
<script>
    // by Mark at http://stackoverflow.com/a/14919494
    function formatBytes(bytes, si) {
        var thresh = si ? 1000 : 1024;
        if(bytes < thresh) return bytes + ' B';
        var units = si ? ['kB','MB','GB','TB','PB','EB','ZB','YB'] : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
        var u = -1;
        do {
            bytes /= thresh;
            ++u;
        } while(bytes >= thresh);
        return bytes.toFixed(1)+' '+units[u];
    }

    var container = document.getElementById("dummy-chart");

    var graph = Flotr.draw(container, [
    [#list seriesList as series]
        {
            data: [[0, ${(series.warcSize)?c}]],
            label: "${series.name}",
        },
    [/#list]
    ], {
        HtmlText: false,
        grid: {
            verticalLines: false,
            horizontalLines: false
        },
        xaxis: {
            showLabels: false
        },
        yaxis: {
            showLabels: false
        },
        pie: {
            show: true,
            explode: 6,
            labelFormatter : function (pie, slice) {
                return formatBytes(slice, true);
            }
        },
        /*
        mouse: {
            track: true
        },*/
        legend: {
            position: 'se',
            backgroundColor: '#D2E8FF'
        }});

</script>
[/@page]