(function(exports) {

  function ccg_scrips_drift(d) {
    return parseInt(d.gp_scrips_drift, 10)
  }

  function chartMeUp(div, csv) {
    var svg = d3.select("#" + div).append('svg')
                .attr("width", 600)
                .attr("height", 480)

    var minx = d3.min(csv, ccg_scrips_drift)
      , maxx = d3.max(csv, ccg_scrips_drift)

    var xscale = d3.scale.linear()
                    .range([ 0, 600 ])
                    .domain([minx, maxx])

    var layout = d3.layout.histogram()
                   .bins(xscale.ticks(100))
                   .value(ccg_scrips_drift)
                   .frequency(true)
                   (csv)
          
    var yscale = d3.scale.linear()
                   .range([0, 450])
                   .domain([ 0, d3.max(layout, function(d) { return d.y }) ])

    var bars = svg.selectAll(".bar")
                 .data(layout)
                 .enter().append("rect")
                  .attr("class", "bar")
                  .attr("x", function(d) { return xscale(d.x)})
                  .attr("y", function(d) { return 450 - yscale(d.y)})
                  .attr("width", xscale(layout[0].dx + minx))
                  .attr("height", function(d) { return yscale(d.y) })
  }

  exports.prescriptionDistribution =  function(div) {
    d3.csv("data/adhd-gp-scrips-drift.csv", function(csv) {
      chartMeUp(div, csv);
    })
  }

})(window)
