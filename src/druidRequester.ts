/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

import { Readable } from 'stream';
import * as Promise from 'any-promise';
import * as request from 'request';
import * as requestPromise from 'request-promise-any';
import * as Agent from 'socks5-http-client/lib/Agent';
import * as Combo from 'stream-json/Combo';
import { RowBuilder } from './rowBuilder';

export interface Location {
  hostname: string;
  port?: number;
}

export interface PlywoodLocator {
  (): Promise<Location>;
}

export interface DruidRequesterParameters {
  locator?: PlywoodLocator;
  host?: string;
  timeout?: number;
  urlBuilder?: DruidUrlBuilder;
  requestDecorator?: DruidRequestDecorator;
}

export interface DruidUrlBuilder {
  (location: Location): string
}

export interface DruidRequestDecorator {
  (decoratorRequest: DecoratorRequest, decoratorContext: { [k: string]: any }): Decoration | Promise<Decoration>;
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
  let queryDataSource = query.dataSource;
  if (!queryDataSource) return [];
  if (typeof queryDataSource === 'string') {
    return [queryDataSource];
  } else if (queryDataSource.type === "union") {
    return queryDataSource.dataSources;
  } else {
    throw new Error(`unsupported datasource type '${queryDataSource.type}'`);
  }
}

function basicLocator(host: string): PlywoodLocator {
  let hostnamePort = host.split(':');
  let hostname: string;
  let port: number;
  if (hostnamePort.length > 1) {
    hostname = hostnamePort[0];
    port = Number(hostnamePort[1]);
  } else {
    hostname = hostnamePort[0];
    port = 8082;
  }
  return () => {
    return Promise.resolve({
      hostname: hostname,
      port: port
    })
  }
}

function basicUrlBuilder(location: Location): string {
  return `http://${location.hostname}:${location.port || 8082}`;
}

interface RequestResponse {
  response: any;
  body: any;
}

// function requestAsPromise(param: request.Options): Promise<RequestResponse> {
//   return new Promise((resolve, reject) => {
//     request(param, (err, response, body) => {
//       if (err) {
//         reject(err);
//       } else {
//         resolve({
//           response: response,
//           body: body
//         });
//       }
//     });
//   });
// }

interface RequestPromiseWithDecorationOptions {
  query: any;
  context?: any;
  options: requestPromise.OptionsWithUrl;
}

export function druidRequesterFactory(parameters: DruidRequesterParameters): Requester.PlywoodRequester<any> {
  let { locator, host, timeout, urlBuilder, requestDecorator } = parameters;
  if (!locator) {
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host);
  }
  if (!urlBuilder) {
    urlBuilder = basicUrlBuilder;
  }

  function requestPromiseWithDecoration(opt: RequestPromiseWithDecorationOptions): Promise<any> {
    return Promise.resolve()
      .then(() => {
        const { query, context, options } = opt;

        // ToDo: is socks
        //options.agentClass = Agent;

        if (requestDecorator) {
          let decorationPromise = requestDecorator({
            method: options.method,
            url: options.url,
            query
          }, context['decoratorContext']);

          if (decorationPromise) {
            return Promise.resolve(decorationPromise)
              .then((decoration: Decoration) => {
                if (decoration.headers) {
                  options.headers = decoration.headers;
                }
                return options;
              });
          }
        }

        return options;
      })
      .then(requestPromise);
  }

  function failIfNoDatasource(url: string, query: any, timeout: number): Promise<any> {
    return requestPromiseWithDecoration({
      query: { queryType: "sourceList" },
      context: {},
      options: {
        method: "GET",
        url: url + "/druid/v2/datasources",
        json: true,
        timeout: timeout
      }
    })
      .then((resp): any => {
        if (getDataSourcesFromQuery(query).every((dataSource) => resp.indexOf(dataSource) < 0)) {
          let err: any = new Error("No such datasource");
          err.query = query;
          throw err;
        }

        return null;
      }, (err) => {
        err.query = query;
        throw err;
      });
  }

  return (req): Readable => {
    let context = req.context || {};
    let query = req.query;
    let { queryType, intervals } = query;

    let stream = new Readable({
      objectMode: true,
      read: function() {
        //connection && connection.resume();
      }
    });

    // Little hack: allow these special intervals to perform a query-less return
    if (intervals === "1000-01-01/1000-01-02") {
      process.nextTick(() => {
        stream.push(null);
      });
      return stream;
    }

    let url: string;
    locator()
      .then((location) => {
        url = urlBuilder(location);

        if (queryType === "status") {
          requestPromiseWithDecoration({
            query,
            context,
            options: {
              method: "GET",
              url: url + '/status',
              json: true,
              timeout: timeout
            }
          })
            .then(
              (resp) => {
                stream.push(resp);
                stream.push(null);
              },
              (reason) => {
                stream.emit('error', reason);
                stream.push(null);
              }
            );

          return;
        }

        if (queryType === "introspect" || queryType === "sourceList") {
          requestPromiseWithDecoration({
            query,
            context,
            options: {
              method: "GET",
              url: url + "/druid/v2/datasources/" + (queryType === "introspect" ? getDataSourcesFromQuery(query)[0] : ''),
              json: true,
              timeout: timeout
            }
          })
            .then((resp) => {
              if (queryType === "introspect") {
                if (Array.isArray(resp.dimensions) && !resp.dimensions.length &&
                  Array.isArray(resp.metrics) && !resp.metrics.length) {

                  return failIfNoDatasource(url, query, timeout).then((): any => {
                    let err: any = new Error("Can not use GET route, data is probably in a real-time node or more than a two weeks old. Try segmentMetadata instead.");
                    err.query = query;
                    throw err;
                  });
                }
              }
              return resp;
            })
            .then(
              (resp) => {
                stream.push(resp);
                stream.push(null);
              },
              (reason) => {
                stream.emit('error', reason);
                stream.push(null);
              }
            );

          return;
        }

        // ========= Must be a data query =========

        if (timeout != null) {
          query.context || (query.context = {});
          query.context.timeout = timeout;
        }

        request({
          method: "POST",
          url: url + "/druid/v2/" + (context['pretty'] ? "?pretty" : ""),
          body: JSON.stringify(query),
          headers: {
            "Content-type": "application/json"
          },
          timeout: timeout
        })
          .on('response', (response) => {
            if (response.statusCode !== 200) {
              stream.emit('error', new Error('bad status code'));
              stream.push(null);
              return;
            }

            response.on('data', (c: any) => console.log('c', c.toString()));
            response.on('end', () => console.log('end'));

            const rq = response
              .pipe(new Combo({ packKeys: true, packStrings: true, packNumbers: true }))
              .pipe(new RowBuilder({}));

            rq.on('error', (e: any) => stream.emit('error', e));
            rq.on('data', (c: any) => stream.push(c));
            rq.on('end', () => stream.push(null));
          });

      });
      /*
      .then((options: request.Options) => {
        let response = result.response;
        let body = result.body;
        let err: any;
        if (response.statusCode !== 200) {
          if (body && body.error === "Query timeout") {
            err = new Error("timeout");
          } else {
            let message: string;
            if (body && typeof body.error === 'string') {
              message = body.error;
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
        if (err.message === 'ETIMEDOUT' || err.message === 'ESOCKETTIMEDOUT') err = new Error("timeout");
        err.query = query;
        throw err;
      });
      */

    return stream;
  };
}
