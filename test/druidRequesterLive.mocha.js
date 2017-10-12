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
  host: info.liveDruidHost,
  socksHost: 'localhost'
});

describe("Druid requester live data source", function() {
  this.timeout(10000);

  it("introspects single dataSource", () => {
    return toArray(druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": 'wikipedia'
      }
    }))
      .then((res) => {
        expect(res[0].dimensions).be.an('Array');
        expect(res[0].metrics).be.an('Array');
      });
  });

  it("introspects multi dataSource", () => {
    return toArray(druidRequester({
      query: {
        "queryType": "introspect",
        "dataSource": {
          "type": "union",
          "dataSources": ['wikipedia', 'wikipedia']
        }
      }
    }))
      .then((res) => {
        expect(res[0].dimensions).be.an('Array');
        expect(res[0].metrics).be.an('Array');
      });
  });

});

