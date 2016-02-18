var { expect } = require("chai");

var { druidRequesterFactory } = require('../build/druidRequester');

var info = require('./info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

describe("Druid requester", function() {
  this.timeout(5 * 1000);

  describe("error", function() {
    it("throws if there is not host or locator", function() {
      expect(() => {
        druidRequesterFactory({});
      }).to.throw('must have a `host` or a `locator`');
    });


    it("correct error for bad datasource", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia_borat'
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.equal("No such datasource");
        return testComplete();
      }
      )
      .done();
    });


    it("correct error for bad datasource that does not exist (on introspect)", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia_borat'
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.equal("No such datasource");
        return testComplete();
      }
      )
      .done();
    });


    it("correct error for bad datasource that do exist (on introspect)", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia'
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.equal("Can not use GET route, data is probably in a real-time node or more than a two weeks old. Try segmentMetadata instead.");
        return testComplete();
      }
      )
      .done();
    });

    return it("correct error for general query error", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeTravel",
          "dataSource": 'wikipedia'
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.contain("Could not resolve type id 'timeTravel' into a subtype of [simple type, class io.druid.query.Query]");
        return testComplete();
      }
      )
      .done();
    });
  });

  describe("introspection", function() {
    it("introspects single data sources", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "sourceList"
        }
      })
      .then(function(res) {
        expect(res).be.an('Array');
        expect(res.indexOf('wikipedia') > -1).to.equal(true);
        return testComplete();
      }
      )
      .done();
    });


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
        return testComplete();
      }
      )
      .done();
    });


    return it("introspects multi dataSource", function(testComplete) {
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
        return testComplete();
      }
      )
      .done();
    });
  });


  describe("basic working", function() {
    it("gets timeBoundary", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia'
        }
      })
      .then(function(res) {
        expect(res.length).to.equal(1);
        expect(isNaN(new Date(res[0].result.maxTime))).to.be.false;
        expect(isNaN(new Date(res[0].result.minTime))).to.be.false;
        return testComplete();
      }
      )
      .done();
    });


    it("works with regular timeseries", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2015-09-12T00:00:00/2015-09-13T00:00:00"]
        }
      })
      .then(function(res) {
        expect(res.length).to.equal(24);
        return testComplete();
      }
      )
      .done();
    });


    it("works with regular time series in the far future", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2045-01-01T00:00:00.000/2045-01-02T00:00:00.000"]
        }
      })
      .then(function(res) {
        expect(res.length).to.equal(0);
        return testComplete();
      }
      )
      .done();
    });


    it("works with regular time series in the far future with invalid data source", function(testComplete) {
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia_borat",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2045-01-01T00:00:00.000/2045-01-02T00:00:00.000"]
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.equal("No such datasource");
        return testComplete();
      }
      )
      .done();
    });
  });


  describe("timeout", function() {
    it("works in simple case", function(testComplete) {
      var timeoutDruidRequester = druidRequesterFactory({
        host: info.druidHost,
        timeout: 50
      });

      timeoutDruidRequester({
        query: {
          "context": {
            //"timeout": 50
            "useCache": false
          },
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2015-09-12T00:00:00/2015-09-13T00:00:00"]
        }
      })
      .then(function() { throw new Error('DID_NOT_ERROR'); })
      .then(null, function(err) {
        expect(err.message).to.equal("timeout");
        return testComplete();
      }
      )
      .done();
    });
  });
});
