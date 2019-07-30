/*
 * Copyright 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

const toArray = require('stream-to-array');

import { druidRequesterFactory } from './druidRequester';

const druidRequester = druidRequesterFactory({
  host: 'localhost:12340', // nothing listening on this port
});

describe('Druid requester dead cluster', function() {
  jest.setTimeout(100);

  it('introspects', () => {
    return toArray(
      druidRequester({
        query: {
          queryType: 'introspect',
          dataSource: 'wikipedia',
        },
      }),
    )
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((e: { message: any }) => {
        expect(e.message).toContain('ECONNREFUSED 127.0.0.1:12340');
      });
  });

  it('query', () => {
    return toArray(
      druidRequester({
        query: {
          queryType: 'timeseries',
          dataSource: 'wikipedia',
          granularity: 'hour',
          aggregations: [{ type: 'count', name: 'Count' }],
          intervals: ['2015-09-12T00:00:00/2015-09-12T05:00:00'],
        },
      }),
    )
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((e: { message: any }) => {
        expect(e.message).toContain('ECONNREFUSED 127.0.0.1:12340');
      });
  });
});
