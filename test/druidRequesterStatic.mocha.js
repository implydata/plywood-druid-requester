/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require("chai");
const toArray = require("stream-to-array");

let { druidRequesterFactory } = require('../build/druidRequester');

let info = require('./info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

describe("Druid requester static data source", function() {
  this.timeout(10000);

  describe("error", () => {
    it("throws if there is not host or locator", () => {
      expect(() => {
        druidRequesterFactory({});
      }).to.throw('must have a `host` or a `locator`');
    });


    it("correct error for bad datasource", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia_borat'
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
        })
    });


    it("correct error for bad datasource that does not exist (on introspect)", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia_borat'
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
        });
    });


    it("correct error for bad datasource that do exist (on introspect)", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia'
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("Can not use GET route, data is probably in a real-time node or more than a two weeks old. Try segmentMetadata instead.");
        })
    });

    it("correct error for general query error", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeTravel",
          "dataSource": 'wikipedia'
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.contain("Could not resolve type id 'timeTravel' into a subtype of [simple type, class io.druid.query.Query]");
        })
    });

  });

  describe("introspection", () => {
    it("introspects single data sources", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "sourceList"
        }
      }))
        .then((res) => {
          expect(res).be.an('Array');
          expect(res.indexOf('wikipedia') > -1).to.equal(true);
        })
    });
  });


  describe("basic working", () => {

    it("gets the status", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "status"
        }
      }))
        .then((res) => {
          expect(res[0].version).to.equal(info.druidVersion);
        })
    });

    it("gets timeBoundary", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(isNaN(new Date(res[0].result.maxTime))).to.equal(false);
          expect(isNaN(new Date(res[0].result.minTime))).to.equal(false);
        })
    });

    it("works with regular timeseries", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2015-09-12T00:00:00/2015-09-12T05:00:00"]
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(5);
          expect(res).to.deep.equal([
            {
              "result": {
                "Count": 2662
              },
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "result": {
                "Count": 11391
              },
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "result": {
                "Count": 10986
              },
              "timestamp": new Date('2015-09-12T02:00:00.000Z')
            },
            {
              "result": {
                "Count": 8109
              },
              "timestamp": new Date('2015-09-12T03:00:00.000Z')
            },
            {
              "result": {
                "Count": 8206
              },
              "timestamp": new Date('2015-09-12T04:00:00.000Z')
            }
          ]);
        })
    });

    it("works with regular topN", () => {
      return toArray(druidRequester({
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
          "metric": 'RowCount',
          "threshold": 6
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(6);
          expect(res).deep.to.equal([
            {
              "result": {
                "RowCount": 317,
                "page": "Jeremy Corbyn"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "result": {
                "RowCount": 255,
                "page": "User:Cyde/List of candidates for speedy deletion/Subpage"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "result": {
                "RowCount": 228,
                "page": "Wikipedia:Administrators' noticeboard/Incidents"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "result": {
                "RowCount": 186,
                "page": "Wikipedia:Vandalismusmeldung"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "result": {
                "RowCount": 163,
                "page": "Total Drama Presents: The Ridonculous Race"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "result": {
                "RowCount": 146,
                "page": "Wikipedia:Administrator intervention against vandalism"
              },
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            }
          ]);
        })
    });

    it.only("works with granularity topN", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "topN",
          "dataSource": "wikipedia",
          "intervals": "2015-09-12/2015-09-12T02:00:00Z",
          "granularity": "hour",
          "context": {
            "useCache": false,
            "populateCache": false
          },
          "aggregations": [{ "name": "RowCount", "type": "count" }],
          "dimension": 'page',
          "metric": 'RowCount',
          "threshold": 2
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(4);
          expect(res).deep.to.equal([
            {
              "result": {
                "RowCount": 10,
                "page": "Israel Ballet"
              },
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "result": {
                "RowCount": 10,
                "page": "POOP"
              },
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "result": {
                "RowCount": 26,
                "page": "Campeonato Mundial de Voleibol Femenino Sub-20 de 2015"
              },
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "result": {
                "RowCount": 22,
                "page": "Flüchtlingskrise in Europa 2015"
              },
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            }
          ]);
        })
    });

    it("works with regular groupBy", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "groupBy",
          "dataSource": "wikipedia",
          "intervals": "2015-09-12/2015-09-13T01:00:00Z",
          "granularity": "all",
          "context": {
            "useCache": false,
            "populateCache": false
          },
          "aggregations": [{ "name": "RowCount", "type": "count" }],
          "dimensions": ['page'],
          "limitSpec": {
            "columns": [
              {
                "dimension": "page"
              }
            ],
            "limit": 10,
            "type": "default"
          }
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(5);
        })
    });

    it("works with regular select", () => {
      return toArray(druidRequester({
        query: {
          // ...
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(5);
        })
    });

    it("works with timeseries with complex agg", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" },
            {
              "fieldName": "delta_hist",
              "name": "!H_P95",
              "type": "approxHistogramFold"
            }
          ],
          "postAggregations": [
            {
              "fieldName": "!H_P95",
              "name": "P95",
              "probability": 0.95,
              "type": "quantile"
            }
          ],
          "intervals": ["2015-09-12T00:00:00/2015-09-13T00:00:00"]
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(24);
        })
    });

    it("works with regular time series in the far future", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2045-01-01T00:00:00.000/2045-01-02T00:00:00.000"]
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(0);
        })
    });

    it("works with regular time series in the far future with invalid data source", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia_borat",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": ["2045-01-01T00:00:00.000/2045-01-02T00:00:00.000"]
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource");
        })
    });
  });


  describe("timeout", () => {
    it("works in simple case", () => {
      let timeoutDruidRequester = druidRequesterFactory({
        host: info.druidHost,
        timeout: 50
      });

      return toArray(timeoutDruidRequester({
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
      }))
        .then((res) => {
          console.log(res);
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("timeout");
        });
    });
  });
});
