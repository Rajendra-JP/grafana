define([
  'lodash'
],
function (_) {
  'use strict';

  function PinotQueryBuilder(target) {
    this.target = target;
  }

  // TODO: rename `tag` to `field|dimension`
  function renderTagCondition (tag, index) {
    var str = "";
    var operator = tag.operator;
    var value = tag.value;
    if (index > 0) {
      str = (tag.condition || 'AND') + ' ';
    }

    if (!operator) {
      if (/^\/.*\/$/.test(tag.value)) {
        operator = '=~';
      } else {
        operator = '=';
      }
    }

    // quote value unless regex
    if (operator !== '=~' && operator !== '!~') {
      value = "'" + value + "'";
    }

    return str + '"' + tag.key + '" ' + operator + ' ' + value;
  }

  var p = PinotQueryBuilder.prototype;

  p.build = function() {
    return this.target.rawQuery ? this._modifyRawQuery() : this._buildQuery();
  };

  p.buildExploreQuery = function(type, withKey) {
    throw {
      name : "NotImplementedError",
      message : "buildExploreQuery not supported for Pinot, type: " + type + " withKey:" + withKey
    };
  };

  p._buildQuery = function() {
    var target = this.target;

    if (!target.measurement) {
      throw "Metric measurement is missing";
    }

    if (!target.fields) {
      throw "Target fields are missing";
    }

    var query = 'SELECT timestamp, '; // TODO(jbetz): Allow developers to specify time field
    var i;
    for (i = 0; i < target.fields.length; i++) {
      var field = target.fields[i];
      if (i > 0) {
        query += ', ';
      }
      // TODO(jbetz): figure out how to enable bucketing.  Once enabled, re-enable functions here.
      //query += field.func + '("' + field.name + '")';
      query += field.name;
    }

    var measurement = target.measurement;
    if (!measurement.match('^/.*/') && !measurement.match(/^merge\(.*\)/)) {
      measurement = '"' + measurement+ '"';
    }

    query += ' FROM ' + measurement + ' WHERE ';
    var conditions = _.map(target.tags, function(tag, index) {
      return renderTagCondition(tag, index);
    });

    query += conditions.join(' ');
    query += (conditions.length > 0 ? ' AND ' : '') + '$timeFilter';

    query += " LIMIT 10000";

    // TODO(jbetz): Add time interval support
    /*query += ' GROUP BY time($interval)';
    if  (target.groupByTags && target.groupByTags.length > 0) {
      query += ', "' + target.groupByTags.join('", "') + '"';
    }*/

    // TODO(jbetz): figure out what to do here.
    /*if (target.fill) {
      query += ' fill(' + target.fill + ')';
    }*/

    target.query = query;

    return query;
  };

  p._modifyRawQuery = function () {
    var query = this.target.query.replace(";", "");
    return query;
  };

  return PinotQueryBuilder;
});
