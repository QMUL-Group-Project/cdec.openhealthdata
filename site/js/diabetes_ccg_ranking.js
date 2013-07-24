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
                fill = e.selectedShape.attr("fill"),
                opacity = e.selectedShape.attr("opacity");

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
                .attr("id", "tooltip")
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

            /* Drop line that goes from the circle to x- and y-axis. Shown on hover. */
            var dropDest = myChart.series[0]._dropLineOrigin(),
                animDuration = 750;

            if (myChart._tooltipGroup !== null && myChart._tooltipGroup !== undefined) {
                myChart._tooltipGroup.remove();
            }
            myChart._tooltipGroup = myChart.svg.append("g");

            // Add a ring around the data point
            myChart._tooltipGroup.append("circle")
                .attr("id", "ring")
                .attr("cx", cx)
                .attr("cy", cy)
                .attr("r", r)
                .attr("opacity", 0)
                .transition()
                .duration(animDuration / 2)
                .ease("linear")
                .attr("opacity", 1)
                .attr("r", r + 4)
                .style("stroke-width", 2);

            // Add a drop line to the y axis
            if (dropDest.x !== null) {
                myChart._tooltipGroup.append("line")
                    .attr("id", "drop")
                    .attr("x1", (cx < dropDest.x ? cx + r + 4 : cx - r - 4))
                    .attr("y1", cy)
                    .attr("x2", (cx < dropDest.x ? cx + r + 4 : cx - r - 4))
                    .attr("y2", cy)
                    .style("opacity", opacity)
                    .transition()
                    .delay(animDuration / 2)
                    .duration(animDuration / 2)
                    .ease("linear")
                    .attr("x2", dropDest.x);
            }

            // Add a drop line to the y axis
            if (dropDest.y !== null) {
                myChart._tooltipGroup.append("line")
                    .attr("id", "drop")
                    .attr("x1", cx)
                    .attr("y1", (cy < dropDest.y ? cy + r + 4 : cy - r - 4))
                    .attr("x2", cx)
                    .attr("y2", (cy < dropDest.y ? cy + r + 4 : cy - r - 4))
                    .style("opacity", opacity)
                    .transition()
                    .delay(animDuration / 2)
                    .duration(animDuration / 2)
                    .ease("linear")
                    .attr("y2", dropDest.y);
            }
        }

        // Event to handle mouse exit
        function onLeave(e) {
            // Remove the popup
            if (popup !== null) {
                popup.remove();
            }
            // Remove the drop line and ring around circle
            if (myChart._tooltipGroup !== null && myChart._tooltipGroup !== undefined) {
                myChart._tooltipGroup.remove();
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
                .attr("id", "tooltip")
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

            /* Drop line that goes from the circle to x- and y-axis. Shown on hover. */
            var dropDest = myChart.series[0]._dropLineOrigin(),
                animDuration = 750;

            if (myChart._tooltipGroup !== null && myChart._tooltipGroup !== undefined) {
                myChart._tooltipGroup.remove();
            }
            myChart._tooltipGroup = myChart.svg.append("g");

            // Add a ring around the data point
            myChart._tooltipGroup.append("circle")
                .attr("id", "ring")
                .attr("cx", cx)
                .attr("cy", cy)
                .attr("r", r)
                .attr("opacity", 0)
                .transition()
                .duration(animDuration / 2)
                .ease("linear")
                .attr("opacity", 1)
                .attr("r", r + 4)
                .style("stroke-width", 2);

            // Add a drop line to the y axis
            if (dropDest.x !== null) {
                myChart._tooltipGroup.append("line")
                    .attr("id", "drop")
                    .attr("x1", (cx < dropDest.x ? cx + r + 4 : cx - r - 4))
                    .attr("y1", cy)
                    .attr("x2", (cx < dropDest.x ? cx + r + 4 : cx - r - 4))
                    .attr("y2", cy)
                    .style("opacity", opacity)
                    .transition()
                    .delay(animDuration / 2)
                    .duration(animDuration / 2)
                    .ease("linear")
                    .attr("x2", dropDest.x);
            }

            // Add a drop line to the y axis
            if (dropDest.y !== null) {
                myChart._tooltipGroup.append("line")
                    .attr("id", "drop")
                    .attr("x1", cx)
                    .attr("y1", (cy < dropDest.y ? cy + r + 4 : cy - r - 4))
                    .attr("x2", cx)
                    .attr("y2", (cy < dropDest.y ? cy + r + 4 : cy - r - 4))
                    .style("opacity", opacity)
                    .transition()
                    .delay(animDuration / 2)
                    .duration(animDuration / 2)
                    .ease("linear")
                    .attr("y2", dropDest.y);
            }
        }

        // Event to handle mouse exit
        function onLeave(e) {
            // Remove the popup
            if (popup !== null) {
                popup.remove();
            }
            // Remove the drop line and ring around circle
            if (myChart._tooltipGroup !== null && myChart._tooltipGroup !== undefined) {
                myChart._tooltipGroup.remove();
            }
        };
    });

}
