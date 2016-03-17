/// <reference path="../typings/request/request.d.ts" />
/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../definitions/locator.d.ts" />
/// <reference path="../definitions/requester.d.ts" />
/// <reference path="../definitions/druid.d.ts" />
"use strict";

import request = require('request');
import Q = require('q');

export interface DruidRequesterParameters {
  locator?: Locator.PlywoodLocator;
  host?: string;
  timeout: number;
}

function getDataSourcesFromQuery(query: Druid.Query): string[] {
  var queryDataSource = query.dataSource;
  if (!queryDataSource) return [];
  if (typeof queryDataSource === 'string') {
    return [queryDataSource];
  } else if (queryDataSource.type === "union") {
    return queryDataSource.dataSources;
  } else {
    throw new Error("unsupported datasource type '" + queryDataSource.type + "'");
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
  var locator = parameters.locator;
  if (!locator) {
    var host = parameters.host;
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host);
  }
  var timeout = parameters.timeout;

  return (req): Q.Promise<any[]> => {
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
        if (queryType === "status") {
          return {
            method: "GET",
            url: url + '/status',
            json: true,
            timeout: timeout
          };
        } else {
          url += '/druid/v2/';
          if (queryType === "introspect" || queryType === "sourceList") {
            return {
              method: "GET",
              url: url + "datasources/" + (queryType === "introspect" ? getDataSourcesFromQuery(query)[0] : ''),
              json: true,
              timeout: timeout
            };
          } else {
            return {
              method: "POST",
              url: url + (context['pretty'] ? "?pretty" : ""),
              json: query,
              timeout: timeout
            };
          }
        }
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
            err = new Error((body && typeof body.error === 'string') ? body.error : 'Bad status code');
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
