define([
  'angular',
],
function (angular) {
  'use strict';

  var module = angular.module('grafana.directives');

  module.directive('metricQueryEditorDruid', function() {
    return {controller: 'DruidTargetCtrl', templateUrl: 'app/plugins/datasource/druid/partials/query.editor.html'};
  });

  module.directive('metricQueryOptionsDruid', function() {
    return {templateUrl: 'app/plugins/datasource/druid/partials/query.options.html'};
  });
});
