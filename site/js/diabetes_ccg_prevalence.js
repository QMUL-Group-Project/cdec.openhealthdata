var ccg_diabetes_prevalence_map = function ccg_diabetes_prevalence_map(div_map, div_sidebar) {

    var chart_svg;
    var map = L.map(div_map).setView([53.0, -1.5], 6);
    var color = function getColor(d) {
        return d > 9 ? '#0C2C84' :
            d > 7 ? '#225EA8' :
            d > 5 ? '#1D91C0' :
            d > 3 ? '#41B6C4' :
            d > 1 ? '#7FCDBB' :
            d > 0 ? '#C7E9B4' :
            '#FFFFCC';
    };
    var style = function style(feature) {
        return {
            fillColor: color(feature.properties.ccg_prevalence),
            weight: 2,
            opacity: 1,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.7
        }
    };
    var defaultStyle = function defaultstyle(feature) {
        return {
            outlineColor: "#000000",
            outlineWidth: 0.5,
            weight: 1,
            opacity: 1,
            fillOpacity: 0
        };
    };
    var pointToLayer = function pointToLayer(feature, latlng) {
        // e.layer.bindPopup('<h4>Hello ' + e.properties.name + '!</h4>');
        return L.circleMarker(latlng, {
            radius: 8,
            fillColor: "#ff7800",
            color: "#000",
            weight: 1,
            opacity: 1,
            fillOpacity: 0.8
        });
    };
    var onEachFeature = function onEachFeature(feature, layer) {
        layer.on({
            mouseover: highlightFeature,
            mouseout: resetHighlight,
            click: zoomToFeature,
            pointToLayer: pointToLayer
        });
    };

    L.tileLayer('http://{s}.tile.cloudmade.com/{key}/22677/256/{z}/{x}/{y}.png',
        {
            attribution: 'Map data &copy; 2011 OpenStreetMap contributors, Imagery &copy; 2012 CloudMade',
            key: 'BC9A493B41014CAABB98F0471D759707'
        }).addTo(map);

    mergedFeatureLayer(map, "data/gp_ccg_prevalence.csv", "data/ccg-boundaries.json", "ccg_code", style, onEachFeature, pointToLayer, "ccg_boundaries");

    addLegend([0, 1, 3, 5, 7, 9], map, color);

    addInfo(map, function (props) {
        var infoBox = '<h3> CCG Diabetes Prevalence </h3><br/>' +
            'CCG Name: '+ props.ccg_name +  '<br />' +
            'CCG Code: ' + props.ccg_code + '<br />' +
            'Registered Patients: ' + numberWithCommas(Math.round(props.ccg_registered_patients)) + '<br />' +
            'Prevalence: ' + Math.round(props.ccg_prevalence * 10) / 10 + '%<br />';
        return infoBox;
    });

    function highlightFeature(e) {
        var layer = e.target;

        layer.setStyle({
            weight: 5,
            color: '#666',
            dashArray: '',
            fillOpacity: 0.7
        });

        if (!L.Browser.ie && !L.Browser.opera) {
            layer.bringToFront();
        }
        e.target._map.info.update(layer.feature.properties);
        var ccg_code = e.target.feature.properties.ccg_code;

        /* Sidebar chart */
        chart_svg = dimple.newSvg(div_sidebar, 300, 300);
        d3.csv("./data/ranking_ccg_bottom10_gp.csv", function (data) {
            data = dimple.filterData(data, "CCG Code", [ccg_code]);
            var sidechart = new dimple.chart(chart_svg, data);
            sidechart.setBounds(60, 30, 200, 200)
            var x = sidechart.addCategoryAxis("x", "Practice Code");
            x.addOrderRule("Date");
            sidechart.addMeasureAxis("y", "GP Prevalence");
            sidechart.addSeries(["Practice Code", "Practice Name"], dimple.plot.bar);
            sidechart.draw();
        });
    }

    function resetHighlight(e) {
        var layer = e.target;
        layer.setStyle(style(e.target.feature));
        e.target._map.info.update();

        // Remove sidebar chart on mouse out
        chart_svg.remove();

    }

    function zoomToFeature(e) {
        e.target._map.fitBounds(e.target.getBounds());
    }
}