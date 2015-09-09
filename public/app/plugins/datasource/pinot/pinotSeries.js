define([
  'lodash',
],
function (_) {
  'use strict';

  function PinotSeries(options, intervalMillis, fromTime, toTime) {
    this.series = options.series;
    this.intervalMillis = intervalMillis;
    this.fromTime = fromTime;
    this.toTime = toTime;
    this.timeColumn = "timestamp"; // TODO(jbetz): Allow developers to specify time dimension field name
  }

  var p = PinotSeries.prototype;

  p.getTimeSeries = function() {
    var output = [];
    var self = this;

    if (self.series.length === 0) {
      return output;
    }
    console.log("getTimeSeries: "); // [{"columns":["duration","timestamp"],"results":[]}]
    console.log(self.series);

    // series - a selectionResults, see: https://github.com/linkedin/pinot/wiki/Pinot-Client-API#selection
    _.each(self.series, function(series) {
      var seriesName = "duration.avg"; // TODO(jbetz): Allow series to be named / aliased ?

      var columns = series.columns;
      var timeColumnIdx = -1;
      console.log("columns: " + columns);
      for (var c = 0; c < columns.length; c++) {
        if (columns[c] === self.timeColumn) {
          timeColumnIdx = c;
          break;
        }
      }

      for (var j = 0; j < columns.length; j++) {
        if (j === timeColumnIdx) {
          continue;
        }

        var datapoints = [];
        if (series.results) {
          for (var i = 0; i < series.results.length; i++) {
            // value, timestamp tuple, e.g. : [0.64, "2015-01-29T21:51:28.968422294Z"]
            datapoints[i] = [parseFloat(series.results[i][j]), parseInt(series.results[i][timeColumnIdx])];
          }
        }
        console.log("column: " + columns[j]);
        var grouped = self._groupByTime(datapoints, self.intervalMillis, self.fromTime, self.toTime);
        console.log("grouped: ");
        console.log(grouped);
        output.push({ target: seriesName, datapoints: grouped});
      }
    });
    console.log("Pinot output:");
    console.log(output);
    return output;
  };

  // TODO(jbetz): Remove. this is not a viable option.
  p._groupByTime = function(datapoints, intervalMillis, fromTime, toTime) {
    console.log("groupByTime: interval: " + this.intervalMillis + " from: " + fromTime + " to: " + toTime);
    datapoints = datapoints.sort(function(a, b) { return a[1] > b[1];});

    var results = [];
    var idx = 0;
    for (var i = fromTime; i < toTime; i += intervalMillis) {
      var values = [];
      var bucketUpperBound = i + intervalMillis;
      for(; idx < datapoints.length && datapoints[idx][1] < bucketUpperBound; idx++) {
        values.push(datapoints[idx][0]);
      }

      if(values.length > 0) {
        var sum = values.reduce(function(a, b) { return a + b; });
        var avg = sum / values.length;
        results.push([avg, i]);
      } else {
        results.push([null, i]);
      }
    }
    return results;
  };

  p.getAnnotations = function () {
    // unsupported
    var list = [];
    return list;
  };

  return PinotSeries;
});
