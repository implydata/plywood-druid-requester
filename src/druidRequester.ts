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
    port = 8080;
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
    if (Array.isArray(query.intervals) && query.intervals.length === 1 && query.intervals[0] === "1000-01-01/1000-01-02") {
      return Q([]);
    }

    var url: string;
    return locator()
      .then((location) => {
        if (timeout != null) {
          query.context || (query.context = {});
          query.context.timeout = timeout;
        }

        url = "http://" + location.hostname + ":" + (location.port || 8080) + "/druid/v2/";
        return {
          method: "POST",
          url: url + (context['pretty'] ? "?pretty" : ""),
          json: query,
          timeout: timeout
        };
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
            err = new Error("Bad status code");
            err.query = query;
          }
          throw err;
        }

        if (typeof body !== 'object') {
          throw new Error("bad response");
        }

        if (Array.isArray(body) && !body.length) {
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
                err = new Error("Bad response");
                err.query = query;
                throw err;
              }

              if (getDataSourcesFromQuery(query).every((dataSource) => body.indexOf(dataSource) < 0)) {
                err = new Error("No such datasource");
                err.query = query;
                throw err;
              }

              return [];
            }, (err) => {
              err.query = query;
              throw err;
            });
        }

        return body;
      }, (err) => {
        if (err.message === "ETIMEDOUT") err = new Error("timeout");
        err.query = query;
        throw err;
      });
  };
}
