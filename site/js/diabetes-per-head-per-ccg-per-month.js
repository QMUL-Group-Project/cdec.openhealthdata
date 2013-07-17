var diabetes_per_head_per_ccg_per_month = function diabetes_per_head_per_ccg_per_month(div) {

    var map = L.map(div).setView([53.0, -1.5], 6);
    var color = function getColor(d) {
        return  d == 'NA' ? '#333' :
            d == 'undefined' ? '#333' :
            d > 30 ? '#91003F' :
            d > 28 ? '#CE1256' :
            d > 26 ? '#E7298A' :
            d > 24 ? '#DF65B0' :
            d > 22 ? '#C994C7' :
            d > 20 ? '#D4B9DA' :
            '#F1EEF6';
    };
    var style = function style(feature) {
        return {
            fillColor: color(feature.properties.per_capita_spend),
            weight: 2,
            opacity: 0.5,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.8
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

    mergedFeatureLayer(map, "data/diabetes_per_head_per_ccg_per_month.csv", "data/ccg-boundaries.json", "ccg_code", style, onEachFeature, pointToLayer, "ccg_boundaries");

    addLegend([0, 20, 22, 24, 26, 28, 30, 34], map, color);

    addInfo(map, function (props) {
        var infoBox = '<h3> CCG Population </h3><br/>' +
            'CCG code: ' + props.ccg_code + '<br/>'
            + 'Registered Patients: ' + props.registered_patients + '<br/>'
            + 'Diabetes Patients: ' + props.diabetes_patients + '<br/>'
            + 'Total Spend: ' + props.total_spend + '<br/>'
            + 'Per Capita Spend: ' + props.per_capita_spend ;
        return infoBox;
    });

    function highlightFeature(e) {
        var layer = e.target;

        layer.setStyle({
            weight: 5,
            color: '#666',
            dashArray: '',
            fillOpacity: 0.6
        });

        if (!L.Browser.ie && !L.Browser.opera) {
            layer.bringToFront();
        }
        map.info.update(layer.feature.properties);
    }

    function resetHighlight(e) {
        var layer = e.target;
        layer.setStyle(style(e.target.feature));
        map.info.update();
    }

    function zoomToFeature(e) {
        map.fitBounds(e.target.getBounds());
    }
}
