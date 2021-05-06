/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2018 Imply Data, Inc.
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
import * as requestPromise from 'request-promise-native';
import { Cancel, CancelToken } from 'axios';
import * as concat from 'concat-stream';
import * as PlainAgent from 'socks5-http-client/lib/Agent';
import * as SecureAgent from 'socks5-https-client/lib/Agent';
import * as Combo from 'stream-json/Combo';
import { RowBuilder } from './rowBuilder';

export type Protocol = 'plain' | 'tls-loose' | 'tls';

export interface DruidUrlBuilder {
  (location: Location, secure: boolean): string
}

export interface DruidRequestDecorator {
  (decoratorRequest: DecoratorRequest, decoratorContext: { [k: string]: any }): Decoration | Promise<Decoration>;
}

export interface DruidRequesterParameters {
  locator?: PlywoodLocator;
  host?: string;
  timeout?: number;
  protocol?: Protocol;
  ca?: string;
  cert?: any;
  key?: any;
  passphrase?: string;
  urlBuilder?: DruidUrlBuilder;
  requestDecorator?: DruidRequestDecorator;
  authToken?: AuthToken;
  socksHost?: string;
  socksUsername?: string;
  socksPassword?: string;
  cancelToken?: CancelToken;
  getQueryId?: () => string;
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
  timestampOverride?: string;
}

function getDataSourcesFromQuery(query: any): string[] {
  let queryDataSource = query.dataSource;
  if (!queryDataSource) return [];
  if (typeof queryDataSource === 'string') {
    return [queryDataSource];
  } else if (queryDataSource.type === "table") {
    return [queryDataSource.name];
  } else if (queryDataSource.type === "union") {
    return queryDataSource.dataSources;
  } else if (queryDataSource.type === "query") {
    return getDataSourcesFromQuery(queryDataSource.query);
  } else {
    // If we can not find a data source, that is ok, avoid the data source existence check
    return [];
  }
}

function basicUrlBuilder(location: Location, secure: boolean): string {
  let s = '';
  let defaultPort = 8082;
  if (secure) {
    s = 's';
    defaultPort += 200;
  }
  return `http${s}://${location.hostname}:${location.port || defaultPort}`;
}

interface RequestWithDecorationOptions {
  query: any;
  context?: any;
  options: request.OptionsWithUrl;
}

export function applyAuthTokenToHeaders(headers: Record<string, string>, authToken: AuthToken | undefined): void {
  if (!authToken) return;

  switch (authToken.type) {
    case 'basic-auth':
      if (typeof authToken.username !== 'string') throw new Error('basic-auth must set username');
      if (typeof authToken.password !== 'string') throw new Error('basic-auth must set password');

      headers["Authorization"] = "Basic " + Buffer.from(authToken.username + ':' + authToken.password).toString('base64');
      break;

    case 'imply-token-hmac':
      if (typeof authToken.implyToken !== 'string') throw new Error('imply-token-hmac must set implyToken');
      if (typeof authToken.implyHmac !== 'string') throw new Error('imply-token-hmac must set implyHmac');

      headers["X-Imply-Token"] = authToken.implyToken;
      headers["X-Imply-HMAC"] = authToken.implyHmac;

      // Temp send headers without X also
      headers["Imply-Token"] = authToken.implyToken;
      headers["Imply-HMAC"] = authToken.implyHmac;
      break;

    case 'bearer-auth':
      if (typeof authToken.bearerToken !== 'string') throw new Error('bearer-auth must set implyIdentityToken');
      headers["Authorization"] = authToken.bearerToken;
      break;

    default:
      throw new Error(`unknown auth token type '${authToken.type}'`);
  }
}

export function druidRequesterFactory(parameters: DruidRequesterParameters): PlywoodRequester<any> {
  let { locator, host, timeout, protocol, urlBuilder, requestDecorator, authToken, socksHost, cancelToken, getQueryId } = parameters;

  if (!protocol) protocol = 'plain';
  const secure = protocol === 'tls' || protocol === 'tls-loose';

  if (!locator) {
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host, secure ? 8282 : 8082);
  }

  if (!urlBuilder) {
    urlBuilder = basicUrlBuilder;
  }

  let agentClass: any = null;
  let agentOptions: any = null;
  if (socksHost) {
    const socksLocation = hostToLocation(socksHost, 1080);
    agentClass = secure ? SecureAgent : PlainAgent;
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

        if (secure) {
          options.strictSSL = (protocol === 'tls');
          if (parameters.ca) options.ca = parameters.ca;
          if (parameters.cert) options.cert = parameters.cert;
          if (parameters.key) options.key = parameters.key;
          if (parameters.passphrase) options.passphrase = parameters.passphrase;
        }

        options.headers = options.headers || {};
        applyAuthTokenToHeaders(options.headers, authToken);

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
                  (options as any).resultType = decoration.resultType; // This is a type hack, ToDo: make proper type here
                }
                if (decoration.timestampOverride) {
                  (options as any).timestampOverride = decoration.timestampOverride; // This is a type hack, ToDo: make proper type here
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
        const dataSourcesInQuery = getDataSourcesFromQuery(query);
        if (dataSourcesInQuery.every((dataSource) => resp.indexOf(dataSource) < 0)) {
          throw new Error(`No such datasource '${dataSourcesInQuery[0]}'`);
        }

        return null;
      });
  }

  return (req): ReadableStream => {
    let context = req.context || {};
    let query = req.query;

    if (getQueryId) {
      query = Object.assign({}, query, {
        context: Object.assign({}, query.context || {}, { queryId: getQueryId() })
      });
    }

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

    function streamError(e: Error | Cancel) {
      (e as any).query = query;
      (e as any).queryId = (query.context || {}).queryId;
      stream.emit('error', e);
      stream.end();
    }

    let url: string;
    locator()
      .then((location) => {
        url = urlBuilder(location, secure);

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

        const queryId = (query.context || {}).queryId;

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
              if (cancelToken) {
                if (cancelToken.reason) {
                  streamError(cancelToken.reason);
                  return;
                }

                if (queryId) {
                  cancelToken.promise.then(() => {
                    return requestPromise({
                      method: "DELETE",
                      url: url + "/druid/v2/" + queryId,
                    });
                  }).catch(() => {}) // Don't worry node about it if it fails
                }
              }

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
                    resultFormat: query.resultFormat,
                    timestamp: (options as any).timestampOverride || (hasOwnProperty(context, 'timestamp') ? context['timestamp'] : 'timestamp'),
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
