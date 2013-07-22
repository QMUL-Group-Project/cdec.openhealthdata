var diabetes_ccg_ranking_top_10 = function diabetes_ccg_ranking_top_10(div) {

    /*  Dimple bubble chart */
    var svg = dimple.newSvg(div, 960, 400);

    d3.csv("./data/ranking_ccg_bottom10.csv", function (data) {

        var myChart = new dimple.chart(svg, data);

        myChart.setBounds(60, 30, 600, 300)
        var x = myChart.addCategoryAxis("x", "CCG Code");
        x.addOrderRule("Prevalence", false); // (ordering measure, descending?)
        myChart.addMeasureAxis("y", "Prevalence");
        myChart.addMeasureAxis("z", "Registered");
        var s = myChart.addSeries(["CCG Code", "CCG Name", "Registered", "Prevalence"], dimple.plot.bubble);
        // Color of bubbles is set by changing 'circle' value in css

        // Handle the hover event - overriding the default behaviour
        s.addEventHandler("mouseover", onHover);
        // Handle the leave event - overriding the default behaviour
        s.addEventHandler("mouseleave", onLeave);

        myChart.draw();

        // Event to handle mouse enter
        function onHover(e) {

            // Get the properties of the selected shape
            var cx = parseFloat(e.selectedShape.attr("cx")),
                cy = parseFloat(e.selectedShape.attr("cy")),
                r = parseFloat(e.selectedShape.attr("r")),
                fill = e.selectedShape.attr("fill");

            // Set the size and position of the popup
            var width = 150,
                height = 100,
                x = (cx + r + width + 10 < svg.attr("width") ?
                    cx + r + 10 :
                    cx - r - width - 20);
            y = (cy - height / 2 < 0 ?
                15 :
                cy - height / 2);

            // Create a group for the popup objects
            popup = svg.append("g");

            var textLength = measureText(e.seriesValue[1], 10, "font-family: sans-serif");

            // Add a rectangle surrounding the text
            popup
                .append("rect")
                .attr("x", x + 5)
                .attr("y", y - 5)
                .attr("width", textLength.width + 75)
                .attr("height", height)
                .attr("rx", 5)
                .attr("ry", 5);

            // Add multiple lines of text
            popup
                .append('text')
                .attr('x', x + 10)
                .attr('y', y + 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 20)
                .text('CCG Code: ' + e.seriesValue[0])
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 40)
                .text('CCG Name: ' + e.seriesValue[1])
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 60)
                .text('Registered patients: ' + numberWithCommas(Math.round(e.seriesValue[2])))
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 80)
                .text('Prevalence: ' + Math.round(e.seriesValue[3] * 10) / 10 + '%')
                .style("font-family", "sans-serif")
                .style("font-size", 10);
        }

        // Event to handle mouse exit
        function onLeave(e) {
            // Remove the popup
            if (popup !== null) {
                popup.remove();
            }
        };
    });
}

var diabetes_ccg_ranking_bottom_10 = function diabetes_ccg_ranking_bottom_10(div) {

    /*  Dimple bubble chart */
    var svg = dimple.newSvg(div, 960, 500);

    d3.csv("./data/ranking_ccg_top10.csv", function (data) {

        var myChart = new dimple.chart(svg, data);

        myChart.setBounds(60, 30, 600, 300)
        var x = myChart.addCategoryAxis("x", "CCG Code");
        x.addOrderRule("Prevalence", true); // (ordering measure, descending?)
        myChart.addMeasureAxis("y", "Prevalence");
        myChart.addMeasureAxis("z", "Registered");
        var s = myChart.addSeries(["CCG Code", "CCG Name", "Registered", "Prevalence"], dimple.plot.bubble);
        // Color of bubbles is set by changing 'circle' value in css

        // Handle the hover event - overriding the default behaviour
        s.addEventHandler("mouseover", onHover);
        // Handle the leave event - overriding the default behaviour
        s.addEventHandler("mouseleave", onLeave);

        myChart.draw();

        // Event to handle mouse enter
        function onHover(e) {

            // Get the properties of the selected shape
            var cx = parseFloat(e.selectedShape.attr("cx")),
                cy = parseFloat(e.selectedShape.attr("cy")),
                r = parseFloat(e.selectedShape.attr("r")),
                fill = e.selectedShape.attr("fill");

            // Set the size and position of the popup
            var width = 150,
                height = 100,
                x = (cx + r + width + 10 < svg.attr("width") ?
                    cx + r + 10 :
                    cx - r - width - 20);
            y = (cy - height / 2 < 0 ?
                15 :
                cy - height / 2);

            // Create a group for the popup objects
            popup = svg.append("g");

            var textLength = measureText(e.seriesValue[1], 10, "font-family: sans-serif");

            // Add a rectangle surrounding the text
            popup
                .append("rect")
                .attr("x", x + 5)
                .attr("y", y - 5)
                .attr("width", textLength.width + 75)
                .attr("height", height)
                .attr("rx", 5)
                .attr("ry", 5)

            // Add multiple lines of text
            popup
                .append('text')
                .attr('x', x + 10)
                .attr('y', y + 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 20)
                .text('CCG Code: ' + e.seriesValue[0])
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 40)
                .text('CCG Name: ' + e.seriesValue[1])
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 60)
                .text('Registered patients: ' + numberWithCommas(Math.round(e.seriesValue[2])))
                .style("font-family", "sans-serif")
                .style("font-size", 10)
                .append('tspan')
                .attr('x', x + 10)
                .attr('y', y + 80)
                .text('Prevalence: ' + Math.round(e.seriesValue[3] * 10) / 10 + '%')
                .style("font-family", "sans-serif")
                .style("font-size", 10);
        }

        // Event to handle mouse exit
        function onLeave(e) {
            // Remove the popup
            if (popup !== null) {
                popup.remove();
            }
        };
    });

}
