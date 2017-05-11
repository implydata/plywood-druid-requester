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
  resultType: string;
  timestamp?: string | null;
  ignorePrefix?: string | null;
  dummyPrefix?: string | null;
}

export class RowBuilder extends Transform {

  static cleanupIgnoreFactory(ignorePrefix: string | null) {
    if (ignorePrefix == null) return null;
    const ignorePrefixLength = ignorePrefix.length;
    return (obj: any) => {
      for (let k in obj) {
        if (k.substr(0, ignorePrefixLength) === ignorePrefix) delete obj[k];
      }
    }
  }

  static cleanupDummyFactory(dummyPrefix: string | null) {
    if (dummyPrefix == null) return null;
    const dummyPrefixLength = dummyPrefix.length;
    return (obj: any) => {
      for (let k in obj) {
        if (k.substr(0, dummyPrefixLength) === dummyPrefix) {
          obj[k.substr(dummyPrefixLength)] = obj[k];
          delete obj[k];
        }
      }
    }
  }

  private assembler: Assembler;
  private flushRoot: boolean;
  private metaEmitted: boolean;
  public maybeNoDataSource: boolean;

  constructor(options: RowBuilderOptions) {
    options.readableObjectMode = true;
    options.writableObjectMode = true;
    super(options);
    const { resultType, timestamp = 'timestamp', ignorePrefix = null, dummyPrefix = null } = options;
    this.maybeNoDataSource = resultType !== 'sql'; // sql mode will always throw an error, thank god.

    const cleanupIgnore = RowBuilder.cleanupIgnoreFactory(ignorePrefix);
    const cleanupDummy = RowBuilder.cleanupDummyFactory(dummyPrefix);

    let onArrayPush: (value: any, stack: any[], keyStack?: ObjectIndex[]) => boolean = null;
    let onKeyValueAdd: (key: ObjectIndex, value: any, stack?: any[], keyStack?: ObjectIndex[]) => boolean = (key, value) => {
      this.maybeNoDataSource = false;
      return true;
    };

    switch (resultType) {
      case 'timeseries':
      case 'timeBoundary':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 0) {
            let d = value.result;
            if (timestamp) d[timestamp] = new Date(value.timestamp);
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        break;

      case 'topN':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 2 && keyStack[1] === 'result') {
            let d = value;
            if (timestamp) d.timestamp = new Date(stack[1].timestamp);
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        break;

      case 'groupBy':
        onArrayPush = (value, stack, keyStack) => {
          if (keyStack.length === 0) {
            let d = value.event;
            if (timestamp) d[timestamp] = new Date(value.timestamp);
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        break;

      case 'select':
        onArrayPush = (value, stack, keyStack) => {
          // keyStack = [0, result, events]
          if (keyStack.length === 3 && keyStack[2] === 'events') {
            let d = value.event;
            if (timestamp) d[timestamp] = new Date(d.timestamp);
            if (timestamp !== 'timestamp') delete d['timestamp'];
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        onKeyValueAdd = (key, value) => {
          this.maybeNoDataSource = false;
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
            let d = value;
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
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

  public _transform(chunk: any, encoding: any, callback: any) {
    this.assembler.process(chunk);
    callback();
  }

  public _flush(callback: any) {
    if (this.flushRoot) {
      this.push(this.assembler.current);
    }
    callback();
  }
}

