/*
 * Copyright 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

import { PlywoodRequester } from 'plywood-base-api';

const toArray = require('stream-to-array');
const cloneDeepWith = require('lodash.clonedeepwith');

import { DecoratorRequest, druidRequesterFactory } from './druidRequester';

const nock = require('nock');

describe('Druid requester intercept', function() {
  it('works with a simple intercept GET', () => {
    const druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
    });

    nock('http://a.druid.host:8082')
      .get('/druid/v2/datasources/dsz')
      .reply(200, {
        dimensions: ['lol'],
        measures: [],
      });

    return toArray(
      druidRequester({
        query: {
          queryType: 'introspect',
          dataSource: 'dsz',
        },
      }),
    ).then((res: any[]) => {
      expect(res.length).toEqual(1);
      expect(res[0]).toEqual({
        dimensions: ['lol'],
        measures: [],
      });
    });
  });

  it('works with a simple intercept POST', () => {
    const druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
    });

    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        queryType: 'topN',
        dataSource: 'dsz',
      })
      .reply(200, [
        {
          timestamp: '2015-01-01T01:01:01Z',
          result: [
            {
              lol: 'data',
            },
          ],
        },
      ]);

    return toArray(
      druidRequester({
        query: {
          queryType: 'topN',
          dataSource: 'dsz',
        },
      }),
    ).then((res: any[]) => {
      expect(res.length).toEqual(1);
      expect(res[0]).toEqual({
        timestamp: new Date('2015-01-01T01:01:01Z'),
        lol: 'data',
      });
    });
  });

  it('works with a different URL builder', () => {
    const druidRequester = druidRequesterFactory({
      host: 'localhost',
      urlBuilder: (location: { hostname: any }) => {
        return `http://${location.hostname}/proxy`;
      },
    });

    nock('http://localhost')
      .post('/proxy/druid/v2/', {
        queryType: 'topNz',
        dataSource: 'dsz',
      })
      .reply(200, {
        lol: 'data',
      });

    return toArray(
      druidRequester({
        query: {
          queryType: 'topNz',
          dataSource: 'dsz',
        },
      }),
    ).then((res: any[]) => {
      expect(res.length).toEqual(1);
      expect(res[0]).toEqual({
        lol: 'data',
      });
    });
  });

  it('works with a basic auth token', () => {
    const druidRequester = druidRequesterFactory({
      host: 'localhost',
      authToken: {
        type: 'basic-auth',
        username: 'admin',
        password: 'druid',
      },
    });

    nock('http://localhost:8082', {
      reqheaders: {
        Authorization: 'Basic YWRtaW46ZHJ1aWQ=',
      },
    })
      .post('/druid/v2/', {
        queryType: 'topNz',
        dataSource: 'dsz',
      })
      .reply(200, {
        lol: 'data',
      });

    return toArray(
      druidRequester({
        query: {
          queryType: 'topNz',
          dataSource: 'dsz',
        },
      }),
    ).then((res: any[]) => {
      expect(res.length).toEqual(1);
      expect(res[0]).toEqual({
        lol: 'data',
      });
    });
  });

  describe('with sync requestDecorator', () => {
    let druidRequester: PlywoodRequester<any>;

    beforeAll(() => {
      druidRequester = druidRequesterFactory({
        host: 'a.druid.host',
        requestDecorator: () => {
          return {
            headers: {
              authorization: 'Basic Auth',
              'X-My-Headers': 'My Header value',
            },
          };
        },
      });
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          authorization: 'Basic Auth',
          'X-My-Headers': 'My Header value',
        },
      })
        .post('/druid/v2/', {
          queryType: 'topNz',
          dataSource: 'dsz',
        })
        .reply(200, {
          lol: 'data',
        });

      return toArray(
        druidRequester({
          query: {
            queryType: 'topNz',
            dataSource: 'dsz',
          },
        }),
      ).then((res: any[]) => {
        expect(res.length).toEqual(1);
        expect(res[0]).toEqual({
          lol: 'data',
        });
      });
    });

    it('decorates request for status query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          authorization: 'Basic Auth',
          'X-My-Headers': 'My Header value',
        },
      })
        .get('/status')
        .reply(200, {
          lol: 'data',
        });

      return toArray(
        druidRequester({
          query: {
            queryType: 'status',
          },
        }),
      ).then((res: any[]) => {
        expect(res.length).toEqual(1);
        expect(res[0]).toEqual({
          lol: 'data',
        });
      });
    });
  });

  describe('with async requestDecorator', () => {
    let druidRequester: PlywoodRequester<any>;

    beforeAll(() => {
      druidRequester = druidRequesterFactory({
        host: 'a.druid.host',
        requestDecorator: () => {
          return Promise.resolve({
            headers: {
              authorization: 'Basic Auth',
              'X-My-Headers': 'My Header value',
            },
          });
        },
      });
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082', {
        reqheaders: {
          authorization: 'Basic Auth',
          'X-My-Headers': 'My Header value',
        },
      })
        .post('/druid/v2/', {
          queryType: 'topNz',
          dataSource: 'dsz',
        })
        .reply(200, {
          lol: 'data',
        });

      return toArray(
        druidRequester({
          query: {
            queryType: 'topNz',
            dataSource: 'dsz',
          },
        }),
      ).then((res: any[]) => {
        expect(res.length).toEqual(1);
        expect(res[0]).toEqual({
          lol: 'data',
        });
      });
    });
  });

  describe('with fancy requestDecorator', () => {
    const fancyRequestDecorator = ({ method, url, query }: DecoratorRequest) => {
      if (method === 'POST' && query) {
        delete query.queryType;
        query.superDuperToken = '555';
        query.filter = cloneDeepWith(query.filter, (f: { type: string }) => {
          if (f.type === 'selector') {
            f.type = 'same-same';
          }
          return f;
        });
      }
      return {
        url: url + 'principalId/3246325435',
        query,
        resultType: 'sql', // expect druidsql like results (i.e. simple array of objects)
      };
    };

    const druidRequester = druidRequesterFactory({
      host: 'a.druid.host',
      requestDecorator: fancyRequestDecorator,
    });

    it('decorates request for topNz query', () => {
      nock('http://a.druid.host:8082')
        .post('/druid/v2/principalId/3246325435', {
          aggregations: [
            {
              name: 'Count',
              type: 'count',
            },
          ],
          dataSource: 'diamonds',
          dimensions: [
            {
              dimension: 'color',
              outputName: 'Color',
              type: 'default',
            },
          ],
          filter: {
            type: 'and',
            filters: [
              {
                type: 'same-same',
                dimension: 'color',
                value: 'some_color',
              },
              {
                type: 'same-same',
                dimension: 'country',
                value: 'USA',
              },
            ],
          },
          granularity: 'all',
          intervals: '2015-03-12T00Z/2015-03-19T00Z',
          superDuperToken: '555',
        })
        .reply(200, [
          {
            color: 'some_color',
            tower: 'babel',
          },
        ]);

      return toArray(
        druidRequester({
          query: {
            aggregations: [
              {
                name: 'Count',
                type: 'count',
              },
            ],
            dataSource: 'diamonds',
            dimensions: [
              {
                dimension: 'color',
                outputName: 'Color',
                type: 'default',
              },
            ],
            filter: {
              type: 'and',
              filters: [
                {
                  type: 'same-same',
                  dimension: 'color',
                  value: 'some_color',
                },
                {
                  type: 'same-same',
                  dimension: 'country',
                  value: 'USA',
                },
              ],
            },
            granularity: 'all',
            intervals: '2015-03-12T00Z/2015-03-19T00Z',
            queryType: 'groupBy',
          },
        }),
      ).then((res: any) => {
        expect(res).toEqual([
          {
            color: 'some_color',
            tower: 'babel',
          },
        ]);
      });
    });
  });

  describe('formats nice error', () => {
    const druidRequester: PlywoodRequester<any> = druidRequesterFactory({
      host: 'a.druid.host',
    });
    nock('http://a.druid.host:8082')
      .post('/druid/v2/', {
        queryType: 'topNz',
        dataSource: 'dszz',
      })
      .reply(500, {
        error: 'Unknown exception',
        errorMessage: 'Pool was initialized with limit = 0, there are no objects to take.',
        errorClass: 'java.lang.IllegalStateException',
        host: '1132637d4b54:8083',
      });
    test('formats nice error', () => {
      return toArray(
        druidRequester({
          query: {
            queryType: 'topNz',
            dataSource: 'dszz',
          },
        }),
      )
        .then(() => {
          throw new Error('did not throw');
        })
        .catch((e: { message: any }) => {
          expect(e.message).toEqual(
            'Unknown exception: Pool was initialized with limit = 0, there are no objects to take.',
          );
        });
    });
  });
});
