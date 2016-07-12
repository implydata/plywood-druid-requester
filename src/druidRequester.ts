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

/// <reference path="../typings/request/request.d.ts" />
/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../definitions/locator.d.ts" />
/// <reference path="../definitions/requester.d.ts" />
/// <reference path="../definitions/druid.d.ts" />

import request = require('request');
import Q = require('q');

export interface DruidRequesterParameters {
  locator?: Locator.PlywoodLocator;
  host?: string;
  timeout?: number;
  requestDecorator?: DruidRequestDecorator;
}

export interface DruidRequestDecorator {
  (decoratorRequest: DecoratorRequest, decoratorContext: { [k: string]: any }): Decoration | Q.Promise<Decoration>;
}

export interface DecoratorRequest {
  method: string;
  url: string;
  query: Druid.Query;
}

export interface Decoration {
  headers: { [header: string]: string };
}

function getDataSourcesFromQuery(query: Druid.Query): string[] {
  var queryDataSource = query.dataSource;
  if (!queryDataSource) return [];
  if (typeof queryDataSource === 'string') {
    return [queryDataSource];
  } else if (queryDataSource.type === "union") {
    return queryDataSource.dataSources;
  } else {
    throw new Error(`unsupported datasource type '${queryDataSource.type}'`);
  }
}

function basicLocator(host: string): Locator.PlywoodLocator {
  var hostnamePort = host.split(':');
  var hostname: string;
  var port: number;
  if (hostnamePort.length > 1) {
    hostname = hostnamePort[0];
    port = Number(hostnamePort[1]);
  } else {
    hostname = hostnamePort[0];
    port = 8082;
  }
  return () => {
    return Q({
      hostname: hostname,
      port: port
    })
  }
}

interface RequestResponse {
  response: {
    statusCode: number;
  };
  body: any;
}

function requestAsPromise(param: request.Options): Q.Promise<RequestResponse> {
  var deferred = <Q.Deferred<RequestResponse>>(Q.defer());
  request(param, (err, response, body) => {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve({
        response: response,
        body: body
      });
    }
  });
  return deferred.promise;
}

function locationToURL(location: Locator.Location): string {
  return `http://${location.hostname}:${location.port || 8082}`;
}

function failIfNoDatasource(url: string, query: Druid.Query, timeout: number): Q.Promise<any> {
  return requestAsPromise({
    method: "GET",
    url: url + "datasources",
    json: true,
    timeout: timeout
  })
    .then((result: RequestResponse): any => {
      var response = result.response;
      var body = result.body;
      var err: any;

      if (response.statusCode !== 200 || !Array.isArray(body)) {
        err = new Error("Bad status code in datasource listing");
        err.query = query;
        throw err;
      }

      if (getDataSourcesFromQuery(query).every((dataSource) => body.indexOf(dataSource) < 0)) {
        err = new Error("No such datasource");
        err.query = query;
        throw err;
      }

      return null;
    }, (err) => {
      err.query = query;
      throw err;
    });
}

export function druidRequesterFactory(parameters: DruidRequesterParameters): Requester.PlywoodRequester<Druid.Query> {
  var { locator, host, timeout, requestDecorator } = parameters;
  if (!locator) {
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host);
  }

  return (req): Q.Promise<any> => {
    var context = req.context || {};
    var query = req.query;
    var { queryType, intervals } = query;
    if (intervals === "1000-01-01/1000-01-02") {
      return Q([]);
    }

    var url: string;
    return locator()
      .then((location) => {
        if (timeout != null) {
          query.context || (query.context = {});
          query.context.timeout = timeout;
        }

        url = locationToURL(location);
        var options: request.Options;
        if (queryType === "status") {
          options = {
            method: "GET",
            url: url + '/status',
            json: true,
            timeout: timeout
          };
        } else {
          url += '/druid/v2/';
          if (queryType === "introspect" || queryType === "sourceList") {
            options = {
              method: "GET",
              url: url + "datasources/" + (queryType === "introspect" ? getDataSourcesFromQuery(query)[0] : ''),
              json: true,
              timeout: timeout
            };
          } else {
            options = {
              method: "POST",
              url: url + (context['pretty'] ? "?pretty" : ""),
              json: query,
              timeout: timeout
            };
          }
        }

        if (requestDecorator) {
          var decorationPromise = requestDecorator({
            method: options.method,
            url: options.url,
            query
          }, context['decoratorContext']);

          if (decorationPromise) {
            return Q(decorationPromise).then((decoration: Decoration) => {
              if (decoration.headers) {
                options.headers = decoration.headers;
              }
              return options;
            });
          }
        }

        return options;
      })
      .then(requestAsPromise)
      .then((result: RequestResponse) => {
        var response = result.response;
        var body = result.body;
        var err: any;
        if (response.statusCode !== 200) {
          if (body && body.error === "Query timeout") {
            err = new Error("timeout");
          } else {
            err = new Error((body && typeof body.error === 'string') ? body.error : `Bad status code (${response.statusCode})`);
            err.query = query;
          }
          throw err;
        }

        if (typeof body !== 'object') {
          throw new Error("bad response");
        }

        if (queryType === "introspect") {
          if (Array.isArray(body.dimensions) && !body.dimensions.length &&
              Array.isArray(body.metrics) && !body.metrics.length) {

            return failIfNoDatasource(url, query, timeout).then((): any => {
              err = new Error("Can not use GET route, data is probably in a real-time node or more than a two weeks old. Try segmentMetadata instead.");
              err.query = query;
              throw err;
            });
          }
        } else if (queryType !== "sourceList" && queryType !== "status") {
          if (Array.isArray(body) && !body.length) {
            return failIfNoDatasource(url, query, timeout).then((): any[] => {
              return [];
            });
          }
        }

        return body;
      }, (err) => {
        if (err.message === "ETIMEDOUT") err = new Error("timeout");
        err.query = query;
        throw err;
      });
  };
}
