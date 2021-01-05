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
const cloneDeepWith = require('lodash.clonedeepwith');

let { druidRequesterFactory } = require('../build/druidRequester');

let nock = require('nock');

describe("Druid requester intercept", function() {
  after(() => {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  it("works with a simple intercept POST", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host'
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        "queryType": "topN",
        "dataSource": 'dsz'
      })
      .reply(200, [{
        timestamp: '2015-01-01T01:01:01Z',
        result: [{
          lol: 'data'
        }]
      }]);

    return toArray(druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    }))
      .then((res) => {
        expect(res.length).to.equal(1);
        expect(res[0]).to.deep.equal({
          timestamp: new Date('2015-01-01T01:01:01Z'),
          lol: 'data'
        });
      });
  });

  it("works with getQueryId", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
      getQueryId: () => 'query-id-xyz'
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        "queryType": "topN",
        "dataSource": 'dsz',
        "context": {
          "queryId": 'query-id-xyz',
        }
      })
      .reply(200, [{
        timestamp: '2015-01-01T01:01:01Z',
        result: [{
          lol: 'data'
        }]
      }]);

    return toArray(druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    }))
      .then((res) => {
        expect(res.length).to.equal(1);
        expect(res[0]).to.deep.equal({
          timestamp: new Date('2015-01-01T01:01:01Z'),
          lol: 'data'
        });
      });
  });

  it("works with a different URL builder", () => {
    let druidRequester = druidRequesterFactory({
      host: 'localhost',
      urlBuilder: (location) => {
        return `http://${location.hostname}/proxy`
      }
    });

    nock('http://localhost')
      .post('/proxy/druid/v2/', {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    return toArray(druidRequester({
      query: {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      }
    }))
      .then((res) => {
        expect(res.length).to.equal(1);
        expect(res[0]).to.deep.equal({
          lol: 'data'
        });
      });
  });

  it("works with a basic auth token", () => {
    let druidRequester = druidRequesterFactory({
      host: 'localhost',
      authToken: {
        type: 'basic-auth',
        username: 'admin',
        password: 'druid'
      }
    });

    nock('http://localhost:8082', {
      reqheaders: {
        'Authorization': 'Basic YWRtaW46ZHJ1aWQ=',
      }
    })
      .post('/druid/v2/', {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    return toArray(druidRequester({
      query: {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      }
    }))
      .then((res) => {
        expect(res.length).to.equal(1);
        expect(res[0]).to.deep.equal({
          lol: 'data'
        });
      });
  });

  it("works with a bearer auth token", () => {
    let druidRequester = druidRequesterFactory({
      host: 'localhost',
      authToken: {
        type: 'bearer-auth',
        bearerToken: 'Bearer: abc123def456',
      }
    });

    nock('http://localhost:8082', {
      reqheaders: {
        'Authorization': 'Bearer: abc123def456',
      }
    })
      .post('/druid/v2/', {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    return toArray(druidRequester({
      query: {
        'queryType': 'topNz',
        'dataSource': 'dsz'
      }
    }))
      .then((res) => {
        expect(res.length).to.equal(1);
        expect(res[0]).to.deep.equal({
          lol: 'data'
        });
      });
  });

  context('with sync requestDecorator', () => {
    let druidRequester;

    before(() => {
      druidRequester = druidRequesterFactory({
        host: 'a.druid.host',
        requestDecorator: () => {
          return {
            headers: {
              'authorization': 'Basic Auth',
              'X-My-Headers': 'My Header value'
            }
          }
        }
      });
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          'authorization': 'Basic Auth',
          'X-My-Headers': 'My Header value'
        }
      })
        .post('/druid/v2/', {
          "queryType": "topNz",
          "dataSource": 'dsz'
        })
        .reply(200, {
          lol: 'data'
        });

      return toArray(druidRequester({
        query: {
          "queryType": "topNz",
          "dataSource": 'dsz'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(res[0]).to.deep.equal({
            lol: 'data'
          });
        });
    });

    it('decorates request for status query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          'authorization': 'Basic Auth',
          'X-My-Headers': 'My Header value'
        }
      })
        .get('/status')
        .reply(200, {
          lol: 'data'
        });

      return toArray(druidRequester({
        query: {
          "queryType": "status"
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(res[0]).to.deep.equal({
            lol: 'data'
          });
        });
    });
  });

  context('with async requestDecorator', () => {
    let druidRequester;

    before(() => {
      druidRequester = druidRequesterFactory({
        host: 'a.druid.host',
        requestDecorator: () => {
          return Promise.resolve({
            headers: {
              'authorization': 'Basic Auth',
              'X-My-Headers': 'My Header value'
            }
          });
        }
      });
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          'authorization': 'Basic Auth',
          'X-My-Headers': 'My Header value'
        }
      })
        .post('/druid/v2/', {
          "queryType": "topNz",
          "dataSource": 'dsz'
        })
        .reply(200, {
          lol: 'data'
        });

      return toArray(druidRequester({
        query: {
          "queryType": "topNz",
          "dataSource": 'dsz'
        }
      }))
        .then((res) => {
          expect(res.length).to.equal(1);
          expect(res[0]).to.deep.equal({
            lol: 'data'
          });
        });
    });
  });

  context('with fancy requestDecorator', () => {
    let druidRequester;

    let fancyRequestDecorator = ({ method, url, query }) => {
      if (method === 'POST' && query) {
        delete query.queryType;
        query.superDuperToken = '555';
        query.filter = cloneDeepWith(query.filter, (f) => {
          if (f.type === 'selector') {
            f.type = 'same-same';
          }
          return f;
        });
      }
      return {
        url: url + 'principalId/3246325435',
        query,
        resultType: 'sql' // expect druidsql like results (i.e. simple array of objects)
      }
    };

    druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
      requestDecorator: fancyRequestDecorator
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082')
        .post('/druid/v2/principalId/3246325435', {
          "aggregations": [
            {
              "name": "Count",
              "type": "count"
            }
          ],
          "dataSource": "diamonds",
          "dimensions": [
            {
              "dimension": "color",
              "outputName": "Color",
              "type": "default"
            }
          ],
          "filter": {
            "type": "and",
            "filters": [
              {
                "type": "same-same",
                "dimension": "color",
                "value": "some_color"
              },
              {
                "type": "same-same",
                "dimension": "country",
                "value": "USA"
              }
            ]
          },
          "granularity": "all",
          "intervals": "2015-03-12T00Z/2015-03-19T00Z",
          "superDuperToken": "555"
        })
        .reply(200, [
          {
            "color": 'some_color',
            "tower": 'babel'
          }
        ]);

      return toArray(druidRequester({
        query: {
          "aggregations": [
            {
              "name": "Count",
              "type": "count"
            }
          ],
          "dataSource": "diamonds",
          "dimensions": [
            {
              "dimension": "color",
              "outputName": "Color",
              "type": "default"
            }
          ],
          "filter": {
            "type": "and",
            "filters": [
              {
                "type": "same-same",
                "dimension": "color",
                "value": "some_color"
              },
              {
                "type": "same-same",
                "dimension": "country",
                "value": "USA"
              }
            ]
          },
          "granularity": "all",
          "intervals": "2015-03-12T00Z/2015-03-19T00Z",
          "queryType": "groupBy"
        }
      }))
        .then((res) => {
          expect(res).to.deep.equal([
            {
              "color": "some_color",
              "tower": 'babel'
            }
          ]);
        });
    });
  });

  it("formats plain error", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      })
      .reply(500, {
        "error": "Opps"
      });

    return toArray(druidRequester({
      query: {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      }
    }))
      .then(() => { throw new Error('did not throw') })
      .catch((e) => {
        expect(e.message).to.equal('Opps');
      });
  });

  it("formats nice error", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      })
      .reply(500, {
        "error": "Unknown exception",
        "errorMessage": "Pool was initialized with limit = 0, there are no objects to take.",
        "errorClass": "java.lang.IllegalStateException",
        "host": "1132637d4b54:8083"
      });

    return toArray(druidRequester({
      query: {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      }
    }))
      .then(() => { throw new Error('did not throw') })
      .catch((e) => {
        expect(e.message).to.equal('Unknown exception: Pool was initialized with limit = 0, there are no objects to take.');
      });
  });

});
