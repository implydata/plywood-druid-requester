/*
 * Copyright 2017-2018 Imply Data, Inc.
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

const { expect } = require('chai');
const toArray = require('stream-to-array');

const { druidRequesterFactory } = require('../build/druidRequester');

const druidRequester = druidRequesterFactory({
  host: 'localhost:12340', // nothing listening on this port
});

describe('Druid requester dead cluster', function () {
  this.timeout(100);

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
      .catch(e => {
        expect(e.message).to.contain('ECONNREFUSED 127.0.0.1:12340');
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
      .catch(e => {
        expect(e.message).to.contain('ECONNREFUSED 127.0.0.1:12340');
      });
  });
});
