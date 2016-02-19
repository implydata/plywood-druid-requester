var { expect } = require("chai");

var { druidRequesterFactory } = require('../build/druidRequester');

var info = require('./info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

describe("Druid requester live data source", function() {
  this.timeout(5 * 1000);

  it("introspects single dataSource", function(testComplete) {
    druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": 'wikipedia'
      }
    })
      .then(function(res) {
        expect(res.dimensions).be.an('Array');
        expect(res.metrics).be.an('Array');
        testComplete();
      })
      .done();
  });

  it("introspects multi dataSource", function(testComplete) {
    druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": {
          "type": "union",
          "dataSources": ['wikipedia', 'wikipedia']
        }
      }
    })
      .then(function(res) {
        expect(res.dimensions).be.an('Array');
        expect(res.metrics).be.an('Array');
        testComplete();
      })
      .done();
  });
});

