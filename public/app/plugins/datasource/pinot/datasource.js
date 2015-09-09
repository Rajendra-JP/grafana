define([
  'angular',
  'lodash',
  'kbn',
  './pinotSeries',
  './queryBuilder',
  './directives',
  './queryCtrl',
  './funcEditor',
],
function (angular, _, kbn, PinotSeries, PinotQueryBuilder) {
  'use strict';

  var module = angular.module('grafana.services');

  module.factory('PinotDatasource', function($q, $http, templateSrv) {

    function PinotDatasource(datasource) {
      this.type = 'pinot';
      this.urls = _.map(datasource.url.split(','), function(url) {
        return url.trim();
      });

      this.name = datasource.name;
      this.basicAuth = datasource.basicAuth;

      this.supportAnnotations = false;
      this.supportMetrics = false;
    }

    PinotDatasource.prototype.query = function(options) {
      var timeFilter = getTimeFilter(options);
      var queryTargets = [];

      var fromTime = parseInt(getPinotTime(options.range.from));
      var toTime = parseInt(getPinotTime(options.range.to));
      var intervalMillis = to_millis(options.interval);

      var allQueries = _.map(options.targets, function(target) {
        if (target.hide) { return []; }

        queryTargets.push(target);

        // build query
        var queryBuilder = new PinotQueryBuilder(target);
        var query =  queryBuilder.build();
        query = query.replace(/\$interval/g, (target.interval || options.interval));
        return query;

      }).join("\n");

      // replace grafana variables
      allQueries = allQueries.replace(/\$timeFilter/g, timeFilter);

      // replace templated variables
      allQueries = templateSrv.replace(allQueries, options.scopedVars);

      return this._seriesQuery(allQueries).then(function(data) {
        if (!data) {
          return [];
        } else if (data.aggregationResults && data.aggregationResults.groupByResult) {
          // group aggregation (https://github.com/linkedin/pinot/wiki/Pinot-Client-API#aggregation-with-grouping)
          throw {name : "NotImplementedError", message : "group aggregation not (yet!) supported for Pinot"};
        } else if (data.selectionResults) {
          var seriesList = [];
          // TODO(jbetz): Support queryTargets?
          var targetSeries = new PinotSeries({ series: [data.selectionResults] }, intervalMillis, fromTime, toTime).getTimeSeries();
          for (var y = 0; y < targetSeries.length; y++) {
            seriesList.push(targetSeries[y]);
          }
          var result = { data: seriesList };
          console.log("PinotDatasource.prototype.query result:");
          console.log(result);
          return result;
        } else {
          return [];
        }
      });
    };

    PinotDatasource.prototype.annotationQuery = function(annotation, rangeUnparsed) {
      throw {
        name : "NotImplementedError",
        message : "annotationQuery not supported for Pinot. annotation: " +
          annotation + " rangeUnparsed: " + rangeUnparsed
        };
    };

    PinotDatasource.prototype.metricFindQuery = function (query) {
      throw {name : "NotImplementedError", message : "metricFindQuery not supported for Pinot. query: " + query};
    };

    PinotDatasource.prototype._seriesQuery = function(query) {
      console.log("Pinot series query: " + query);
      var result = this._pinotRequest('POST', '/query', {pql: query});
      console.log("Pinot Series query result: ");
      console.log(result);
      return result;
    };

    PinotDatasource.prototype.testDatasource = function() {
      // TODO: implement
      return Promise.resolve("success").then(function() {
        return { status: "success", message: "Data source is working", title: "Success" };
      });
    };

    PinotDatasource.prototype._pinotRequest = function(method, url, data) {
      var self = this;

      // round robin load balancing I guess...
      var currentUrl = self.urls.shift();
      self.urls.push(currentUrl);

      var options = {
        method: method,
        url:    currentUrl + url,
        data:   data,
        precision: "ms",
        inspect: { type: 'pinot' },
      };

      options.headers = options.headers || {};
      if (self.basicAuth) {
        options.headers.Authorization = self.basicAuth;
      }

      return $http(options).then(function(result) {
        return result.data;
      }, function(err) {
        if (err.status !== 0 || err.status >= 300) {
          if (err.data && err.data.error) {
            throw { message: 'Pinot Error Response: ' + err.data.error, data: err.data, config: err.config };
          }
          else {
            throw { messsage: 'Pinot Error: ' + err.message, data: err.data, config: err.config };
          }
        }
      });
    };

    function getTimeFilter(options) {
      var from = getPinotTime(options.range.from);
      var until = getPinotTime(options.range.to);

      // TODO(jbetz): make 'timestamp' a configurable field so user can set it to whatever `timeFieldSpec` field they are using
      return 'timestamp BETWEEN ' + from + ' AND ' + until;
    }

    function getPinotTime(date) {
      if (_.isString(date)) {
        var timeOffset = parseRelative(date);
        if (timeOffset) {
          return (new Date().getTime() + to_millis(timeOffset)).toFixed(0);
        }
        date = kbn.parseDate(date);
      }
      return to_utc_epoch(date);
    }

    function to_utc_epoch(date) {
      return (date.getTime()).toFixed(0);
    }

    function parseRelative(relativeTime) {
      var match = relativeTime.match(/(now)-([0-9]+[smh])/);
      if (match) {
        return match[2];
      } else {
        return null;
      }
    }

    function to_millis(timeOffset) {
      var match = timeOffset.match(/([0-9]+)([smh])/);
      var value = match[1];
      var timeUnit = match[2];
      if (timeUnit === 's') {
        return value * 1000;
      } else if (timeUnit === 'm') {
        return value * 1000 * 60;
      } else if (timeUnit === 'h') {
        return value * 1000 * 60 * 60;
      } else {
        throw {
          name : "UnsupportedError",
          message : "Unsupported '" + timeUnit + "' unit in timeOffset: " + timeOffset
        };
      }
    }

    return PinotDatasource;

  });

});
