define([
  'angular',
],
function (angular) {
  'use strict';

  var module = angular.module('grafana.directives');

  module.directive('metricQueryEditorPinot', function() {
    return {controller: 'PinotQueryCtrl', templateUrl: 'app/plugins/datasource/pinot/partials/query.editor.html'};
  });

  module.directive('metricQueryOptionsPinot', function() {
    return {templateUrl: 'app/plugins/datasource/pinot/partials/query.options.html'};
  });
});
