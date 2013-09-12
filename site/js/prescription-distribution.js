(function(exports) {

  function drift(d) {
    return parseFloat(d.drift)
  }

  function chartMeUp(div, csv) {

    var svg = d3.select("#" + div).append('svg')
                .attr("width", 600)
                .attr("height", 480)

    var minx = d3.min(csv, drift)
      , maxx = d3.max(csv, drift)
      , bucketcount = 1000

    var step = (maxx - minx) / bucketcount;

    var buckets = []
    for(var i = 0 ; i <= bucketcount ; i++)
      buckets[i] = { count: 0, key: (i * step) + minx, entries: [] }

    for(i =0 ; i < csv.length; i++) {
      var item = csv[i]
        , index = parseInt((drift(item) - minx) / step, 10)
      buckets[index].count++
      buckets[index].entries.push(item.name)
    }

    for(var i = 0 ; i <= bucketcount ; i++)
      if(buckets[i].entries.length > 5)
        buckets[i].entries = []

    var myChart = _chart = new dimple.chart(svg, buckets);
    myChart.setBounds(60, 30, 510, 305)
    var x = myChart.addCategoryAxis("x", ["key", "entries"]);
    x.addOrderRule("key")
    var y = myChart.addMeasureAxis("y", "count");
    myChart.addSeries(null, dimple.plot.bar);
    myChart.draw();
  }

  exports.prescriptionDistribution =  function(div, file) {
    d3.csv(file, function(csv) {
      chartMeUp(div, csv);
    })
  }

})(window)
