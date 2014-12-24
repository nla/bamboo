[@page title="Overview"]
<ul>
    <li><a href="import">Import a crawl from Heritrix</a></li>
</ul>

<div id="dummy-chart" style="width: 400px; height: 400px;"></div>


<h2>Collections</h2>
<ul>
    [#list collections as collection]
        <li><a href="collection/${collection.id}">${collection.name}</a></li>
    [/#list]
</ul>


<script src="webjars/flotr2/d43f8566e8/flotr2.min.js"></script>
<script>
    var container = document.getElementById("dummy-chart");
        var
    d1 = [
        [0, 4]
    ],
        d2 = [
            [0, 3]
        ],
        d3 = [
            [0, 1.03]
        ],
        graph;

    graph = Flotr.draw(container, [{
        data: d1,
        label: 'Whole Domain Harvest',
    }, {
        data: d2,
        label: 'Australian Government Web Archive'
    }, {
        data: d3,
        label: 'Legacy NPH',
    },], {
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
            explode: 6
        },
        mouse: {
            track: true
        },
        legend: {
            position: 'se',
            backgroundColor: '#D2E8FF'
        }});

</script>
[/@page]