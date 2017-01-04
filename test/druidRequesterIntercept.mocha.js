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
let Promise = require("any-promise");

let { druidRequesterFactory } = require('../build/druidRequester');

let nock = require('nock');


describe("Druid requester static data source", function() {

  it("works with a simple intercept GET", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host'
    });

    nock('http://a.druid.host:8082')
      .get('/druid/v2/datasources/dsz')
      .reply(200, {
        dimensions: ['lol'],
        measures: []
      });

    return druidRequester({
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
      });
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
      .reply(200, {
        lol: 'data'
      });

    return druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
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
        'queryType': 'topN',
        'dataSource': 'dsz'
      })
      .reply(200, {
        lol: 'data'
      });

    return druidRequester({
      query: {
        'queryType': 'topN',
        'dataSource': 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
        });
      });
  });

  it("works with simple headers POST (sync)", () => {
    let druidRequester = druidRequesterFactory({
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

    return druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
        });
      });
  });

  it("works with simple headers POST (async)", () => {
    let druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
      requestDecorator: () => {
        return Promise.resolve(() => {
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

    return druidRequester({
      query: {
        "queryType": "topN",
        "dataSource": 'dsz'
      }
    })
      .then((res) => {
        expect(res).to.deep.equal({
          lol: 'data'
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

    return druidRequester({
      query: {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      }
    })
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

    return druidRequester({
      query: {
        "queryType": "groupBy",
        "dataSource": 'dsz'
      }
    })
      .then(() => { throw new Error('did not throw') })
      .catch((e) => {
        expect(e.message).to.equal('Unknown exception: Pool was initialized with limit = 0, there are no objects to take.');
      });
  });

});
