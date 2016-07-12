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

var { expect } = require("chai");
var Q = require("q");

var { druidRequesterFactory } = require('../build/druidRequester');

var nock = require('nock');


describe("Druid requester static data source", function() {

  it("works with a simple intercept GET", (testComplete) => {
    var druidRequester = druidRequesterFactory({
      host: 'a.druid.host'
    });

    nock('http://a.druid.host:8082')
      .get('/druid/v2/datasources/dsz')
      .reply(200, {
        dimensions: ['lol'],
        measures: []
      });

    druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          dimensions: ['lol'],
          measures: []
        });
        testComplete();
      })
      .done();
  });

  it("works with a simple intercept POST", (testComplete) => {
    var druidRequester = druidRequesterFactory({
      host: 'a.druid.host'
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        "queryType": "topN",
        "dataSource": 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
        });
        testComplete();
      })
      .done();
  });

  it("works with simple headers POST (sync)", (testComplete) => {
    var druidRequester = druidRequesterFactory({
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

    nock('http://a.druid.host:8082', {
      reqheaders: {
        'authorization': 'Basic Auth',
        'X-My-Headers': 'My Header value'
      }
    })
      .post('/druid/v2/', {
        "queryType": "topN",
        "dataSource": 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
        });
        testComplete();
      })
      .done();
  });

  it("works with simple headers POST (async)", (testComplete) => {
    var druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
      requestDecorator: () => {
        return Q.delay(30).then(() => {
          return {
            headers: {
              'authorization': 'Basic Auth',
              'X-My-Headers': 'My Header value'
            }
          }
        });
      }
    });

    nock('http://a.druid.host:8082', {
      reqheaders: {
        'authorization': 'Basic Auth',
        'X-My-Headers': 'My Header value'
      }
    })
      .post('/druid/v2/', {
        "queryType": "topN",
        "dataSource": 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
        });
        testComplete();
      })
      .done();
  });

});
