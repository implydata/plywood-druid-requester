var { expect } = require("chai");

var { druidRequesterFactory } = require('../build/druidRequester');

var info = require('./info');

var druidRequester = druidRequesterFactory({
  host: info.liveDruidHost
});

describe("Druid requester live data source", function() {

  it("introspects single dataSource", (testComplete) => {
    druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": 'wikipedia'
      }
    })
      .then((res) => {
        expect(res.dimensions).be.an('Array');
        expect(res.metrics).be.an('Array');
        testComplete();
      })
      .done();
  });

  it("introspects multi dataSource", (testComplete) => {
    druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": {
          "type": "union",
          "dataSources": ['wikipedia', 'wikipedia']
        }
      }
    })
      .then((res) => {
        expect(res.dimensions).be.an('Array');
        expect(res.metrics).be.an('Array');
        testComplete();
      })
      .done();
  });
});

