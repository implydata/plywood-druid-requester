/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2018 Imply Data, Inc.
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
          expect(err.message).to.equal("No such datasource 'wikipedia_borat'");
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
          expect(err.message).to.equal("No such datasource 'wikipedia_borat'");
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

    it("correct error for segmentMetadata", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "segmentMetadata",
          "dataSource": 'wikipedia_borat',
          merge: true,
          analysisTypes: [],
          lenientAggregatorMerge: true
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.equal("No such datasource 'wikipedia_borat'");
        })
    });

    it("correct error for bad SQL (parse)", () => {
      return toArray(druidRequester({
        query: {
          query: "SELECT page, COUNT(*) AS Cnt FROZON"
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.contain(`Unknown exception: Encountered "FROZON" at line 1`);
        })
    });

    it("correct error for bad SQL (no table)", () => {
      return toArray(druidRequester({
        query: {
          query: "SELECT page, COUNT(*) AS Cnt FROM moonshine"
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.contain(`Object 'moonshine' not found`);
        })
    });

    it("correct error for bad SQL (logic)", () => {
      return toArray(druidRequester({
        query: {
          query: `SELECT
            FLUME("count") AS "Count"
            FROM "wikipedia"
            WHERE ("channel" <> 'en')
            GROUP BY ''`
        }
      }))
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.include('No match found for function signature FLUME(<NUMERIC>)');
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
          expect(res.length).to.equal(1);
          expect(res[0]).be.an('Array');
          expect(res[0].indexOf('wikipedia') > -1).to.equal(true);
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
          expect(res.length).to.equal(1);
          expect(res[0].version).to.equal(info.druidVersion);
        })
    });

    it("works with timeBoundary", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeBoundary",
          "dataSource": 'wikipedia'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(res).to.deep.equal([
            {
              "maxTime": "2015-09-12T23:59:00.000Z",
              "minTime": "2015-09-12T00:46:00.000Z",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            }
          ]);
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
              "Count": 2662,
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "Count": 11391,
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "Count": 10986,
              "timestamp": new Date('2015-09-12T02:00:00.000Z')
            },
            {
              "Count": 8109,
              "timestamp": new Date('2015-09-12T03:00:00.000Z')
            },
            {
              "Count": 8206,
              "timestamp": new Date('2015-09-12T04:00:00.000Z')
            }
          ]);
        })
    });

    it("works with regular timeseries with ignorePrefix", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" },
            { "type": "doubleSum", "name": "!lol!_a1", "fieldName": "added" },
            { "type": "doubleSum", "name": "!lol!_a2", "fieldName": "deleted" }
          ],
          "intervals": ["2015-09-12T00:00:00/2015-09-12T05:00:00"]
        },
        context: {
          ignorePrefix: '!lol!'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(5);
          expect(res).to.deep.equal([
            {
              "Count": 2662,
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "Count": 11391,
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "Count": 10986,
              "timestamp": new Date('2015-09-12T02:00:00.000Z')
            },
            {
              "Count": 8109,
              "timestamp": new Date('2015-09-12T03:00:00.000Z')
            },
            {
              "Count": 8206,
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
              "RowCount": 317,
              "page": "Jeremy Corbyn",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "RowCount": 255,
              "page": "User:Cyde/List of candidates for speedy deletion/Subpage",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "RowCount": 228,
              "page": "Wikipedia:Administrators' noticeboard/Incidents",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "RowCount": 186,
              "page": "Wikipedia:Vandalismusmeldung",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "RowCount": 163,
              "page": "Total Drama Presents: The Ridonculous Race",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "RowCount": 146,
              "page": "Wikipedia:Administrator intervention against vandalism",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            }
          ]);
        })
    });

    it("works with granularity topN", () => {
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
              "RowCount": 10,
              "page": "Israel Ballet",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 10,
              "page": "POOP",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 26,
              "page": "Campeonato Mundial de Voleibol Femenino Sub-20 de 2015",
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "RowCount": 22,
              "page": "Flüchtlingskrise in Europa 2015",
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
          "intervals": "2015-09-12/2015-09-12T01:00:00Z",
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
                "dimension": "page",
                "outputName": "$__page"
              }
            ],
            "limit": 5,
            "type": "default"
          }
        },
        context: {
          dummyPrefix: '$__'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(5);
          expect(res).to.deep.equal([
            {
              "RowCount": 1,
              "page": "(90) Antiope",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 1,
              "page": ".aaa",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 1,
              "page": ".abb",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 1,
              "page": ".abbott",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "RowCount": 1,
              "page": ".axa",
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            }
          ]);
        })
    });

    it("works with time groupBy", () => {
      return toArray(druidRequester({
        query: {
          "queryType": "groupBy",
          "dataSource": "wikipedia-compact",
          "intervals": "2015-09-12T00Z/2015-09-13T00Z",
          "granularity": "all",
          "context": {
            "timeout": 10000,
            "groupByStrategy": "v2",
            "useCache": false,
            "populateCache": false
          },
          "filter": {
            "type": "not",
            "field": {
              "type": "selector",
              "dimension": "channel",
              "value": "en"
            }
          },
          "dimensions": [
            {
              "type": "extraction",
              "dimension": "__time",
              "outputName": "***__time",
              "extractionFn": {
                "type": "javascript",
                "function": "function(s){try{\nvar d = new org.joda.time.DateTime(s);\nd = d.hourOfDay().roundFloorCopy();\nd = d.hourOfDay().setCopy(Math.floor(d.hourOfDay().get() / 2) * 2);\nreturn d;\n}catch(e){return null;}}"
              }
            },
            {
              "type": "default",
              "dimension": "channel",
              "outputName": "channel"
            }
          ],
          "aggregations": [
            {
              "name": "Count",
              "type": "doubleSum",
              "fieldName": "count"
            }
          ],
          "limitSpec": {
            "type": "default",
            "columns": [
              {
                "dimension": "Count",
                "direction": "descending"
              }
            ],
            "limit": 4
          }
        },
        context: {
          timestamp: null,
          dummyPrefix: '***'
        }
      }))
        .then((res) => {
          expect(res).to.deep.equal([
            {
              "Count": 24276,
              "__time": "2015-09-12T06:00:00.000Z",
              "channel": "vi"
            },
            {
              "Count": 11223,
              "__time": "2015-09-12T16:00:00.000Z",
              "channel": "vi"
            },
            {
              "Count": 9258,
              "__time": "2015-09-12T14:00:00.000Z",
              "channel": "vi"
            },
            {
              "Count": 8928,
              "__time": "2015-09-12T08:00:00.000Z",
              "channel": "vi"
            }
          ]);
        })
    });

    it("works with regular select", () => {
      const requester = druidRequester({
        query: {
          "queryType": "select",
          "dataSource": "wikipedia",
          "dimensions": [
            "page"
          ],
          "granularity": "all",
          "intervals": "2015-09-12/2015-09-12T02:00:00Z",
          "metrics": [
            "count",
            "added"
          ],
          "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 4
          }
        }
      });

      let seenMeta = false;
      requester.on('meta', (meta) => {
        seenMeta = true;
        expect(meta).to.have.key('pagingIdentifiers');
      });

      return toArray(requester)
        .then((res) => {
          expect(seenMeta).to.equal(true);
          expect(res.length).to.equal(4);
          expect(res).to.deep.equal([
            {
              "added": 0,
              "count": 1,
              "page": "Israel Ballet",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 213,
              "count": 1,
              "page": "Diskussion:Flüchtlingskrise in Europa 2015",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 0,
              "count": 1,
              "page": "Angelika Wende",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 0,
              "count": 1,
              "page": "Talk:Economic growth",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            }
          ]);
        })
    });

    it("works with regular scan (list)", () => {
      const requester = druidRequester({
        query: {
          "queryType": "scan",
          "dataSource": "wikipedia",
          "intervals": "1000/3000",
          "granularity": "all",
          "filter": null,
          "resultFormat": "list",
          "limit": 4,
          "columns": [
            "page",
            "channel",
            "comment",
            "added"
          ]
        }
      });

      return toArray(requester)
        .then((res) => {
          expect(res.length).to.equal(4);
          expect(res).to.deep.equal([
            {
              "added": 0,
              "channel": "ca",
              "comment": "/* Enllaços externs */",
              "page": "Israel Ballet"
            },
            {
              "added": 213,
              "channel": "de",
              "comment": "/* Begriffsklärung */",
              "page": "Diskussion:Flüchtlingskrise in Europa 2015"
            },
            {
              "added": 0,
              "channel": "de",
              "comment": "tk",
              "page": "Angelika Wende"
            },
            {
              "added": 0,
              "channel": "en",
              "comment": "Archiving 1 discussion(s) to [[Talk:Economic growth/Archive 4]]) (bot",
              "page": "Talk:Economic growth"
            }
          ]);
        })
    });

    it("works with regular scan (compactedList)", () => {
      const requester = druidRequester({
        query: {
          "queryType": "scan",
          "dataSource": "wikipedia",
          "intervals": "1000/3000",
          "granularity": "all",
          "filter": null,
          "resultFormat": "compactedList",
          "limit": 4,
          "columns": [
            "page",
            "channel",
            "comment",
            "added"
          ]
        }
      });

      return toArray(requester)
        .then((res) => {
          expect(res.length).to.equal(4);
          expect(res).to.deep.equal([
            {
              "added": 0,
              "channel": "ca",
              "comment": "/* Enllaços externs */",
              "page": "Israel Ballet"
            },
            {
              "added": 213,
              "channel": "de",
              "comment": "/* Begriffsklärung */",
              "page": "Diskussion:Flüchtlingskrise in Europa 2015"
            },
            {
              "added": 0,
              "channel": "de",
              "comment": "tk",
              "page": "Angelika Wende"
            },
            {
              "added": 0,
              "channel": "en",
              "comment": "Archiving 1 discussion(s) to [[Talk:Economic growth/Archive 4]]) (bot",
              "page": "Talk:Economic growth"
            }
          ]);
        })
    });

    it("works with regular select with special timestamp", () => {
      const requester = druidRequester({
        query: {
          "queryType": "select",
          "dataSource": "wikipedia",
          "dimensions": [
            "page"
          ],
          "granularity": "all",
          "intervals": "2015-09-12/2015-09-12T02:00:00Z",
          "metrics": [
            "count",
            "added"
          ],
          "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 4
          }
        },
        context: {
          timestamp: 'time'
        }
      });

      return toArray(requester)
        .then((res) => {
          expect(res.length).to.equal(4);
          expect(res).to.deep.equal([
            {
              "added": 0,
              "count": 1,
              "page": "Israel Ballet",
              "time": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 213,
              "count": 1,
              "page": "Diskussion:Flüchtlingskrise in Europa 2015",
              "time": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 0,
              "count": 1,
              "page": "Angelika Wende",
              "time": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 0,
              "count": 1,
              "page": "Talk:Economic growth",
              "time": new Date('2015-09-12T00:46:00.000Z')
            }
          ]);
        })
    });

    it("works with granularity select", () => {
      const requester = druidRequester({
        query: {
          "queryType": "select",
          "dataSource": "wikipedia",
          "dimensions": [
            "page"
          ],
          "granularity": "hour",
          "intervals": "2015-09-12/2015-09-12T02:00:00Z",
          "metrics": [
            "count",
            "added"
          ],
          "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 2
          }
        }
      });

      let seenMeta = false;
      requester.on('meta', (meta) => {
        seenMeta = true;
        expect(meta).to.have.key('pagingIdentifiers');
      });

      return toArray(requester)
        .then((res) => {
          expect(seenMeta).to.equal(true);
          expect(res.length).to.equal(4);
          expect(res).to.deep.equal([
            {
              "added": 0,
              "count": 1,
              "page": "Israel Ballet",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 213,
              "count": 1,
              "page": "Diskussion:Flüchtlingskrise in Europa 2015",
              "timestamp": new Date('2015-09-12T00:46:00.000Z')
            },
            {
              "added": 12,
              "count": 1,
              "page": "عملية بوجينكا",
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            },
            {
              "added": 39,
              "count": 1,
              "page": "بوليطية",
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            }
          ]);
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
          "intervals": ["2015-09-12T00:00:00/2015-09-12T02:00:00"]
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(2);
          expect(res).deep.to.equal([
            {
              "!H_P95": {
                "breaks": [
                  -19985.333984375,
                  -499,
                  18987.333984375,
                  38473.66796875,
                  57960,
                  77446.3359375,
                  96932.671875,
                  116419
                ],
                "counts": [
                  0,
                  2677,
                  2,
                  1,
                  0,
                  0,
                  1
                ]
              },
              "Count": 2662,
              "P95": 92.884995,
              "timestamp": new Date('2015-09-12T00:00:00.000Z')
            },
            {
              "!H_P95": {
                "breaks": [
                  -28350.5,
                  -498,
                  27354.5,
                  55207,
                  83059.5,
                  110912,
                  138764.5,
                  166617
                ],
                "counts": [
                  0,
                  11429,
                  8,
                  2,
                  0,
                  1,
                  2
                ]
              },
              "Count": 11391,
              "P95": 130.39272,
              "timestamp": new Date('2015-09-12T01:00:00.000Z')
            }
          ]);
        })
    });

    it("works with regular timeseries in the far future", () => {
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

    it("works with regular timeseries in the far future with invalid data source", () => {
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
          expect(err.message).to.equal("No such datasource 'wikipedia_borat'");
        })
    });

    it("works with segmentMetadata (merge)", () => {
      return toArray(druidRequester({
        query: {
          queryType: "segmentMetadata",
          dataSource: "wikipedia",
          merge: true,
          analysisTypes: [],
          lenientAggregatorMerge: true
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(Object.keys(res[0].columns)).to.deep.equal([
            "__time",
            "added",
            "channel",
            "cityName",
            "comment",
            "commentLength",
            "commentLengthStr",
            "commentTerms",
            "count",
            "countryIsoCode",
            "countryName",
            "deleted",
            "delta",
            "deltaBucket100",
            "deltaByTen",
            "delta_hist",
            "geohash",
            "isAnonymous",
            "isMinor",
            "isNew",
            "isRobot",
            "isUnpatrolled",
            "max_delta",
            "metroCode",
            "min_delta",
            "namespace",
            "page",
            "page_unique",
            "regionIsoCode",
            "regionName",
            "sometimeLater",
            "sometimeLaterMs",
            "user",
            "userChars",
            "user_theta",
            "user_unique"
          ]);
        })
    });

    it("works with DruidSQL group by", () => {
      const requester = druidRequester({
        query: {
          query: "SELECT page, COUNT(*) AS Cnt FROM wikipedia GROUP BY page LIMIT 5"
        }
      });

      let seenMeta = false;
      requester.on('meta', (meta) => {
        seenMeta = true;
        expect(meta).to.deep.equal({
          columns: ['page', 'Cnt']
        });
      });

      return toArray(requester)
        .then((res) => {
          expect(seenMeta).to.equal(true);
          expect(res).to.deep.equal([
            {
              "Cnt": 1,
              "page": "!T.O.O.H.!"
            },
            {
              "Cnt": 2,
              "page": "\"The Secret Life of...\""
            },
            {
              "Cnt": 2,
              "page": "'''Kertomus Venetsiasta''' 1977"
            },
            {
              "Cnt": 1,
              "page": "'Ajde Jano"
            },
            {
              "Cnt": 1,
              "page": "'Alî Sharî'atî"
            }
          ]);
        })
    });

    it("works with DruidSQL empty result", () => {
      return toArray(druidRequester({
        query: {
          query: "SELECT page, COUNT(*) AS Cnt FROM wikipedia WHERE cityName = 'lol_lol_lol_lol' GROUP BY page LIMIT 5"
        }
      }))
        .then((res) => {
          expect(res).to.deep.equal([]);
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
