(function(exports) {

  // This is temporary, until I generate a set with the real data in it
  function mangleData(rows, ccgmap) {
    for(var i = 0; i < rows.length; i++) {
      for(var j = 0; j < ccgmap.length; j++) {
        var row = rows[i]
          , ccg = ccgmap[j]
        if(ccg.practice_code === row.id) {
          row.ccg_code = ccg.ccg_code
          row.ccg_name = ccg.ccg_name
        }
      }
    }
    return rows
  }


  var _chart = null

  function generateChartForCcg(svg, ccg, data) {
    data = data.filter(function(i) { return i.ccg_code === ccg })

//    var svg = dimple.newSvg("#chartContainer", 590, 400);

    if(_chart) {
      _chart.data = data
      _chart.draw()
    } else {
      var myChart = _chart = new dimple.chart(svg, data);
      myChart.setBounds(60, 30, 510, 305)
      var x = myChart.addCategoryAxis("x", "name");
      var y = myChart.addMeasureAxis("y", "drift");
      y.overrideMin = -2;  
      y.overrideMax = 10;   
      myChart.addSeries(null, dimple.plot.bar);
      myChart.draw();
    }
  }

  var _data = null
    , _svg =  null

  exports.changeCcgInBreakdown = function(ccg) {
    if(!_data) return setTimeout(function() { changeCcgInBreakdown(ccg) }, 500)
    generateChartForCcg(_svg, ccg, _data)
  }

  exports.ccgBreakdown = function(div) {
    _svg = d3.select('#' + div)
                .append('svg')
                .attr('width', 600)
                .attr('height', 480)

    d3.csv('data/adhd-gp-scrips-drift.csv', function(items) {
      d3.csv('data/gp_ccg_prevalence.csv', function(gpccgmap) {
        _data = mangleData(items, gpccgmap)
      })
    })
  }
})(this)
