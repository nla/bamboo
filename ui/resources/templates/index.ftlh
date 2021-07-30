[@page title="Overview"]

<h3>System Statistics</h3>
<table id="stats-table">
    <tr><th>URL snapshots:</th><td>${warcStatistics.records} records</td></tr>
    <tr><th>WARC files:</th><td>${warcStatistics.files} files; ${warcStatistics.size} bytes (${warcStatistics.displaySize})</td></tr>
    <tr><th>Other artifacts:</th><td>${artifactStatistics.files} files; ${artifactStatistics.size} bytes (${artifactStatistics.displaySize})</td></tr>
    <tr><th>Total storage:</th><td>${totalStatistics.files} files; ${totalStatistics.size} bytes (${totalStatistics.displaySize})</td></tr>
</table>

    <style>
        #stats-table th {
            text-align: right;
            padding-right: 0.5em;
        }
    </style>

[/@page]