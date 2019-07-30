/*
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

import { Transform, TransformOptions } from 'stream';

import { Assembler, ObjectIndex } from './assembler';

export interface RowBuilderOptions extends TransformOptions {
  resultType: string;
  resultFormat?: string;
  timestamp?: string | null;
  ignorePrefix?: string | null;
  dummyPrefix?: string | null;
}

export class RowBuilder extends Transform {
  static cleanupIgnoreFactory(ignorePrefix: string | null) {
    if (ignorePrefix == null) return null;
    const ignorePrefixLength = ignorePrefix.length;
    return (obj: any) => {
      for (const k in obj) {
        if (k.substr(0, ignorePrefixLength) === ignorePrefix) delete obj[k];
      }
    };
  }

  static cleanupDummyFactory(dummyPrefix: string | null) {
    if (dummyPrefix == null) return null;
    const dummyPrefixLength = dummyPrefix.length;
    return (obj: any) => {
      for (const k in obj) {
        if (k.substr(0, dummyPrefixLength) === dummyPrefix) {
          obj[k.substr(dummyPrefixLength)] = obj[k];
          delete obj[k];
        }
      }
    };
  }

  private assembler: Assembler;
  private flushRoot: boolean;
  private metaEmitted: boolean;
  private columns: string[] | undefined;
  public maybeNoDataSource: boolean;

  constructor(options: RowBuilderOptions) {
    options.readableObjectMode = true;
    options.writableObjectMode = true;
    super(options);
    const {
      resultType,
      resultFormat,
      timestamp = 'timestamp',
      ignorePrefix = null,
      dummyPrefix = null,
    } = options;
    this.maybeNoDataSource = resultType !== 'sql'; // sql mode will always throw an error, thank god.
    this.flushRoot = false;
    this.metaEmitted = false;

    const cleanupIgnore = RowBuilder.cleanupIgnoreFactory(ignorePrefix);
    const cleanupDummy = RowBuilder.cleanupDummyFactory(dummyPrefix);

    let onArrayPush: ((value: any, stack: any[], keyStack?: ObjectIndex[]) => boolean) | undefined;
    let onKeyValueAdd: (
      key: ObjectIndex,
      value: any,
      stack?: any[],
      keyStack?: ObjectIndex[],
    ) => boolean = () => {
      this.maybeNoDataSource = false;
      return true;
    };

    switch (resultType) {
      case 'timeseries':
      case 'timeBoundary':
        onArrayPush = (value, _stack, keyStack) => {
          if (!keyStack || keyStack.length === 0) {
            const d = value.result;
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
          if (!keyStack) return true;
          if (keyStack.length === 2 && keyStack[1] === 'result') {
            const d = value;
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
        onArrayPush = (value, _stack, keyStack) => {
          if (!keyStack || keyStack.length === 0) {
            const d = value.event;
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
        onArrayPush = (value, _stack, keyStack) => {
          // keyStack = [0, result, events]
          if (!keyStack) return true;
          if (keyStack.length === 3 && keyStack[2] === 'events') {
            const d = value.event;
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

      case 'scan':
        if (resultFormat === 'compactedList') {
          let columns: string[] | undefined;
          onArrayPush = (value, _stack, keyStack) => {
            // keyStack = [0, events]
            if (!keyStack) return true;
            if (keyStack.length === 2 && keyStack[1] === 'events') {
              const d: any = {};
              const n = columns ? columns.length : 0;
              for (let i = 0; i < n; i++) {
                // @ts-ignore
                d[columns[i]] = value[i];
              }
              if (cleanupIgnore) cleanupIgnore(d);
              if (cleanupDummy) cleanupDummy(d);
              this.push(d);
              return false;
            }
            return true;
          };
          onKeyValueAdd = (key, value, _stack, keyStack) => {
            if (!keyStack) return true;
            if (key !== 'columns' || keyStack.length !== 1) return true;
            columns = value;
            return false;
          };
        } else {
          onArrayPush = (value, _stack, keyStack) => {
            // keyStack = [0, events]
            if (!keyStack) return true;
            if (keyStack.length === 2 && keyStack[1] === 'events') {
              const d = value;
              if (cleanupIgnore) cleanupIgnore(d);
              if (cleanupDummy) cleanupDummy(d);
              this.push(d);
              return false;
            }
            return true;
          };
        }
        break;

      case 'segmentMetadata':
        onArrayPush = (value, _stack, keyStack) => {
          if (!keyStack || keyStack.length === 0) {
            const d = value;
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        break;

      case 'sql':
        this.columns = [];
        onArrayPush = (value, _stack, keyStack) => {
          if (!keyStack || keyStack.length === 0) {
            if (this.columns) {
              this.emit('meta', {
                columns: this.columns,
              });
              this.columns = undefined;
            }

            const d = value;
            if (cleanupIgnore) cleanupIgnore(d);
            if (cleanupDummy) cleanupDummy(d);
            this.push(d);
            return false;
          }
          return true;
        };
        onKeyValueAdd = (key, _value, _stack, keyStack) => {
          if (!this.columns) return true;
          this.maybeNoDataSource = false;
          if (!keyStack) return true;
          if (keyStack.length === 1 && keyStack[0] === 0) {
            this.columns.push(String(key));
          }
          return true;
        };
        break;

      default:
        this.flushRoot = true;
    }

    this.assembler = new Assembler({
      onArrayPush,
      onKeyValueAdd,
    });
  }

  public _transform(chunk: any, _encoding: any, callback: any) {
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
