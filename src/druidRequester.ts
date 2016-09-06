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

/// <reference path="../typings/requester.d.ts" />
/// <reference path="../typings/locator.d.ts" />

import * as request from 'request';
import * as Q from 'q-tsc';

export interface DruidRequesterParameters {
  locator?: Locator.PlywoodLocator;
  host?: string;
  timeout?: number;
  urlBuilder?: DruidUrlBuilder;
  requestDecorator?: DruidRequestDecorator;
}

export interface DruidUrlBuilder {
  (location: Locator.Location): string
}

export interface DruidRequestDecorator {
  (decoratorRequest: DecoratorRequest, decoratorContext: { [k: string]: any }): Decoration | Q.Promise<Decoration>;
}

export interface DecoratorRequest {
  method: string;
  url: string;
  query: any;
}

export interface Decoration {
  headers: { [header: string]: string };
}

function getDataSourcesFromQuery(query: any): string[] {
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

function basicUrlBuilder(location: Locator.Location): string {
  return `http://${location.hostname}:${location.port || 8082}`;
}

interface RequestResponse {
  response: any;
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

function failIfNoDatasource(url: string, query: any, timeout: number): Q.Promise<any> {
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
        err = new Error(`Bad status code (${response.statusCode}) in datasource listing`);
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

export function druidRequesterFactory(parameters: DruidRequesterParameters): Requester.PlywoodRequester<any> {
  var { locator, host, timeout, urlBuilder, requestDecorator } = parameters;
  if (!locator) {
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host);
  }
  if (!urlBuilder) {
    urlBuilder = basicUrlBuilder;
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

        url = urlBuilder(location);
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
            var message: string;
            if (body && typeof body.error === 'string') {
              message = body.errorClass || body.error;
              if (typeof body.errorMessage === 'string') {
                message = `${message}: ${body.errorMessage}`;
              }
            } else {
              message = `Bad status code (${response.statusCode})`;
            }
            err = new Error(message);
            err.query = query;
            if (body && typeof body.host === 'string') err.host = body.host;
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
