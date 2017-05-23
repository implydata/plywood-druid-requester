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

import { PlywoodRequester, PlywoodLocator, Location, basicLocator, hostToLocation, AuthToken } from 'plywood-base-api';
import { ReadableStream, PassThrough } from 'readable-stream';
import * as request from 'request';
import * as hasOwnProperty from 'has-own-prop'
import * as requestPromise from 'request-promise-any';
import * as concat from 'concat-stream';
import * as Agent from 'socks5-http-client/lib/Agent';
import * as Combo from 'stream-json/Combo';
import { RowBuilder } from './rowBuilder';

export interface DruidRequesterParameters {
  locator?: PlywoodLocator;
  host?: string;
  timeout?: number;
  urlBuilder?: DruidUrlBuilder;
  requestDecorator?: DruidRequestDecorator;
  authToken?: AuthToken;
  socksHost?: string;
  socksUsername?: string;
  socksPassword?: string;
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
  method?: string;
  url?: string;
  headers?: Record<string, string>;
  query?: string | object;
  resultType?: string;
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

function basicUrlBuilder(location: Location): string {
  return `http://${location.hostname}:${location.port || 8082}`;
}

interface RequestWithDecorationOptions {
  query: any;
  context?: any;
  options: request.OptionsWithUrl;
}

export function druidRequesterFactory(parameters: DruidRequesterParameters): PlywoodRequester<any> {
  let { locator, host, timeout, urlBuilder, requestDecorator, authToken, socksHost } = parameters;
  if (!locator) {
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host, 8082);
  }
  if (!urlBuilder) {
    urlBuilder = basicUrlBuilder;
  }

  let agentClass: any = null;
  let agentOptions: any = null;
  if (socksHost) {
    const socksLocation = hostToLocation(socksHost, 1080);
    agentClass = Agent;
    agentOptions = {
      socksHost: socksLocation.hostname,
      socksPort: socksLocation.port
    };
    if (parameters.socksUsername) agentOptions.socksUsername = parameters.socksUsername;
    if (parameters.socksPassword) agentOptions.socksPassword = parameters.socksPassword;
  }

  function requestOptionsWithDecoration(opt: RequestWithDecorationOptions): Promise<request.OptionsWithUrl> {
    return Promise.resolve()
      .then(() => {
        const { query, context, options } = opt;
        if (agentClass) {
          options.agentClass = agentClass;
          options.agentOptions = agentOptions;
        }

        options.headers = options.headers || {};
         if (authToken && authToken.type === 'basic') {
           options.headers["Authorization"] = "Basic " + new Buffer(authToken.token).toString('base64');
         }

        if (requestDecorator) {
          let decorationPromise = requestDecorator({
            method: options.method,
            url: options.url,
            query: JSON.parse(JSON.stringify(query)) // quick deep copy
          }, context['decoratorContext']);

          if (decorationPromise) {
            return Promise.resolve(decorationPromise)
              .then((decoration: Decoration) => {
                if (!decoration) return options;
                if (decoration.method) {
                  options.method = decoration.method;
                }
                if (decoration.url) {
                  options.url = decoration.url;
                }
                if (decoration.headers) {
                  Object.assign(options.headers, decoration.headers);
                }
                if (decoration.query) {
                  if (typeof decoration.query === 'string') {
                    options.body = decoration.query;
                  } else {
                    options.body = JSON.stringify(decoration.query);
                  }
                }
                if (decoration.resultType) {
                  // This is a type hack, ToDo: make proper type here
                  (options as any).resultType = decoration.resultType;
                }
                return options;
              });
          }
        }

        return options;
      });
  }

  function requestPromiseWithDecoration(opt: RequestWithDecorationOptions): Promise<any> {
    return requestOptionsWithDecoration(opt).then(requestPromise);
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
          throw new Error("No such datasource");
        }

        return null;
      });
  }

  return (req): ReadableStream => {
    let context = req.context || {};
    let query = req.query;
    let { queryType, intervals } = query;

    // Maybe Druid SQL
    if (!queryType && typeof query.query === 'string') {
      queryType = 'sql';
    }

    let stream = new PassThrough({
      objectMode: true
    });

    // Little hack: allow these special intervals to perform a query-less return
    if (intervals && (intervals === "1000-01-01/1000-01-02" || !intervals.length)) {
      process.nextTick(() => {
        stream.push(null);
      });
      return stream;
    }

    function streamError(e: Error) {
      (e as any).query = query;
      stream.emit('error', e);
      stream.end();
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
              streamError
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
              streamError
            );

          return;
        }

        // ========= Must be a data query =========

        if (timeout != null) {
          query.context || (query.context = {});
          query.context.timeout = timeout;
        }

        requestOptionsWithDecoration({
          query,
          context,
          options: {
            method: "POST",
            url: url + "/druid/v2/" + (queryType === 'sql' ? 'sql/' : '') + (context['pretty'] ? '?pretty': ''),
            body: JSON.stringify(query),
            headers: {
              "Content-type": "application/json"
            },
            timeout: timeout
          }
        })
          .then(
            (options) => {
              request(options)
                .on('error', (err: any) => {
                  if (err.message === 'ETIMEDOUT' || err.message === 'ESOCKETTIMEDOUT') err = new Error("timeout");
                  streamError(err);
                })
                .on('response', (response) => {
                  if (response.statusCode !== 200) {
                    response.on('error', streamError);

                    response.pipe(concat((resp: string) => {
                      resp = String(resp);
                      let error: any;
                      try {
                        const body = JSON.parse(resp);
                        if (body && body.error === "Query timeout") {
                          error = new Error("timeout");
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
                          error = new Error(message);
                          error.query = query;
                          if (body && typeof body.host === 'string') error.host = body.host;
                        }
                      } catch (e) {
                        error = new Error("bad response");
                      }

                      streamError(error);
                    }));
                    return;
                  }

                  // response.on('data', (c: any) => console.log('c', c.toString()));
                  // response.on('end', () => console.log('end'));

                  const rowBuilder = new RowBuilder({
                    resultType: (options as any).resultType || queryType,
                    timestamp: hasOwnProperty(context, 'timestamp') ? context['timestamp'] : 'timestamp',
                    ignorePrefix: context['ignorePrefix'],
                    dummyPrefix: context['dummyPrefix']
                  });

                  rowBuilder.on('meta', (meta: any) => {
                    stream.emit('meta', meta);
                  });

                  rowBuilder.on('end', () => {
                    if (!rowBuilder.maybeNoDataSource) {
                      stream.end();
                      return;
                    }

                    failIfNoDatasource(url, query, timeout)
                      .then(
                        (): any => {
                          stream.end()
                        },
                        streamError
                      );
                  });

                  response
                    .pipe(new Combo({ packKeys: true, packStrings: true, packNumbers: true }))
                    .pipe(rowBuilder)
                    .pipe(stream, { end: false });

                  // rq.on('error', (e: any) => stream.emit('error', e));
                  // rq.on('data', (c: any) => stream.push(c));
                  // rq.on('end', () => stream.push(null));
                });
            },
            streamError
          );

      });

    return stream;
  };
}
