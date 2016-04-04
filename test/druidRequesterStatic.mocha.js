var { expect } = require("chai");

var { druidRequesterFactory } = require('../build/druidRequester');

var info = require('./info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

describe("Druid requester static data source", function() {

  describe("error", () => {
    it("throws if there is not host or locator", () => {
      expect(() => {
        druidRequesterFactory({});
      }).to.throw('must have a `host` or a `locator`');
    });


    it("correct error for bad datasource", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia_borat'
        }
      })
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
          testComplete();
        })
        .done();
    });


    it("correct error for bad datasource that does not exist (on introspect)", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia_borat'
        }
      })
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
          testComplete();
        })
        .done();
    });


    it("correct error for bad datasource that do exist (on introspect)", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia'
        }
      })
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("Can not use GET route, data is probably in a real-time node or more than a two weeks old. Try segmentMetadata instead.");
          testComplete();
        })
        .done();
    });

    it("correct error for general query error", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "timeTravel",
          "dataSource": 'wikipedia'
        }
      })
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
            expect(err.message).to.contain("Could not resolve type id 'timeTravel' into a subtype of [simple type, class io.druid.query.Query]");
            testComplete();
          }
        )
        .done();
    });
  });

  describe("introspection", () => {
    it("introspects single data sources", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "sourceList"
        }
      })
        .then((res) => {
          expect(res).be.an('Array');
          expect(res.indexOf('wikipedia') > -1).to.equal(true);
          testComplete();
        })
        .done();
    });
  });


  describe("basic working", () => {

    it("gets the status", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "status"
        }
      })
        .then((res) => {
          expect(res.version).to.equal("0.8.3-iap1");
          testComplete();
        })
        .done();
    });

    it("gets timeBoundary", (testComplete) => {
      druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia'
        }
      })
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(isNaN(new Date(res[0].result.maxTime))).to.be.false;
          expect(isNaN(new Date(res[0].result.minTime))).to.be.false;
          testComplete();
        })
        .done();
    });


    it("works with regular timeseries", (testComplete) => {
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
        .then((res) => {
          expect(res.length).to.equal(24);
          testComplete();
        })
        .done();
    });


    it("works with regular time series in the far future", (testComplete) => {
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
        .then((res) => {
          expect(res.length).to.equal(0);
          testComplete();
        })
        .done();
    });


    it("works with regular time series in the far future with invalid data source", (testComplete) => {
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
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
          testComplete();
        })
        .done();
    });
  });


  describe("timeout", () => {
    it("works in simple case", (testComplete) => {
      var timeoutDruidRequester = druidRequesterFactory({
        host: info.druidHost,
        timeout: 50
      });

      timeoutDruidRequester({
        query: {
          "queryType": "topN",
          "dataSource": "wikipedia",
          "intervals": "2015-09-12/2015-09-13",
          "granularity": "all",
          "context": {
            "useCache": false,
            "populateCache": false
          },
          "aggregations": [{ "name": "RowCount", "type": "count" }],
          "dimension": 'page',
          "metric": {
            "type": "alphaNumeric"
          },
          "threshold": 1000
        }
      })
        .then((res) => {
          console.log(res);
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("timeout");
          testComplete();
        })
        .done();
    });
  });
});
