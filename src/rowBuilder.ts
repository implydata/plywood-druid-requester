/*
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

import { Transform, TransformOptions } from "stream";
import { Assembler, ObjectIndex } from "./assembler";

export interface RowBuilderOptions extends TransformOptions {
  queryType: string;
  timestamp?: string | null;
  ignorePrefix?: string | null;
}

export class RowBuilder extends Transform {

  static cleanupFactory(ignorePrefix: string | null) {
    if (ignorePrefix == null) return null;
    const ignorePrefixLength = ignorePrefix.length;
    return (obj: any) => {
      for (let k in obj) {
        if (k.substr(0, ignorePrefixLength) === ignorePrefix) delete obj[k];
      }
    }
  }

  private assembler: Assembler;
  private flushRoot: boolean;
  private metaEmitted: boolean;
  public totallyEmpty: boolean;

  constructor(options: RowBuilderOptions) {
    options.readableObjectMode = true;
    options.writableObjectMode = true;
    super(options);
    this.totallyEmpty = true;
    const { queryType, timestamp = 'timestamp', ignorePrefix = null } = options;

    const cleanup = RowBuilder.cleanupFactory(ignorePrefix);

    let onArrayPush: (value: any, stack: any[], keyStack?: ObjectIndex[]) => boolean = null;
    let onKeyValueAdd: (key: ObjectIndex, value: any, stack?: any[], keyStack?: ObjectIndex[]) => boolean = (key, value) => {
      this.totallyEmpty = false;
      return true;
    };

    switch (queryType) {
      case 'timeseries':
      case 'timeBoundary':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 0) {
            let result = value.result;
            if (timestamp) result[timestamp] = new Date(value.timestamp);
            if (cleanup) cleanup(result);
            this.push(result);
            return false;
          }
          return true;
        };
        break;

      case 'topN':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 2 && keyStack[1] === 'result') {
            if (timestamp) value.timestamp = new Date(stack[1].timestamp);
            if (cleanup) cleanup(value);
            this.push(value);
            return false;
          }
          return true;
        };
        break;

      case 'groupBy':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 0) {
            let event = value.event;
            if (timestamp) event[timestamp] = new Date(value.timestamp);
            if (cleanup) cleanup(event);
            this.push(event);
            return false;
          }
          return true;
        };
        break;

      case 'select':
        onArrayPush = (value, stack, keyStack) => {
          // keyStack = [0, result, events]
          if (keyStack.length === 3 && keyStack[2] === 'events') {
            let event = value.event;
            if (timestamp) event[timestamp] = new Date(event.timestamp);
            if (timestamp !== 'timestamp') delete event['timestamp'];
            if (cleanup) cleanup(event);
            this.push(event);
            return false;
          }
          return true;
        };
        onKeyValueAdd = (key, value) => {
          this.totallyEmpty = false;
          if (key !== 'pagingIdentifiers') return true;
          if (this.metaEmitted) return false;
          this.emit('meta', { pagingIdentifiers: value });
          this.metaEmitted = true;
          return false;
        };
        break;

      case 'segmentMetadata':
      case 'sql':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 0) {
            if (cleanup) cleanup(value);
            this.push(value);
            return false;
          }
          return true;
        };
        break;

      default:
        this.flushRoot = true;
    }

    this.assembler = new Assembler({
      onArrayPush,
      onKeyValueAdd
    });
  }

  protected _transform(chunk: any, encoding: any, callback: any) {
    this.assembler.process(chunk);
    callback();
  }

  protected _flush(callback: any) {
    if (this.flushRoot) {
      this.push(this.assembler.current);
    }
    callback();
  }
}

