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

import { Transform } from "stream";
import * as Assembler from "stream-json/utils/Assembler";

type RowBuilderState = 'init' | 'tso-array' | 'tso' | 'tso-ts' | 'result-array' | 'result'; // tso = time stamped object

export class RowBuilder extends Transform {
  private _curTimestamp: Date;
  private _curResult: any;
  private _curKey: string;
  private _curAssembler: Assembler;
  private _curState: RowBuilderState;
  private _stateStack: RowBuilderState[];

  constructor(options: any) {
    options.readableObjectMode = true;
    options.writableObjectMode = true;
    super(options);
    // this._writableState.objectMode = true;
    // this._readableState.objectMode = true;

    this._curState = 'init';
    this._stateStack = [];
    this._curResult = null;
    this._curKey = null;
  }

  private _pushState(newState: RowBuilderState): void {
    this._stateStack.push(this._curState);
    this._curState = newState;
  }

  private _popState(): void {
    this._curState = this._stateStack.pop();
  }

  protected _transform(chunk: any, encoding: any, callback: any) {
    if (this._curAssembler) {
      if (this._curAssembler[chunk.name]) {
        this._curAssembler[chunk.name](chunk.value);
      }

      if (!this._curAssembler.stack.length) {
        this._curResult[this._curKey] = chunk.value;
        this._curAssembler = null;
      }

      callback();
      return;
    }

    switch (chunk.name) {
      case 'startArray':
        switch (this._curState) {
          case 'init':
            this._pushState('tso-array');
            break;

          case 'tso':
            this._pushState('result-array');
            break;

          default:
            throw new Error(`oh noes ${chunk.name} in ${this._curState}`);
        }
        break;

      case 'endArray':
        switch (this._curState) {
          case 'tso-array':
          case 'result-array':
            this._popState();
            break;

          default:
            throw new Error(`oh noes ${chunk.name} in ${this._curState}`);
        }
        break;

      case 'startObject':
        switch (this._curState) {
          case 'tso-array':
            this._pushState('tso');
            break;

          case 'tso':
          case 'result-array':
            this._pushState('result');
            this._curResult = {};
            break;

          case 'result':
            this._curAssembler = new Assembler();
            this._curAssembler[chunk.name](chunk.value);
            break;

          default:
            throw new Error(`oh noes ${chunk.name} in ${this._curState}`);
        }
        break;

      case 'endObject':
        switch (this._curState) {
          case 'tso':
            this._popState();
            break;

          case 'result':
            this.push({ timestamp: this._curTimestamp, result: this._curResult });
            this._popState();
            this._curResult = null;
            break;

          default:
            throw new Error(`oh noes ${chunk.name} in ${this._curState}`);
        }
        break;

      case 'keyValue':
        switch (this._curState) {
          case 'tso':
            if (chunk.value === 'timestamp') {
              this._pushState('tso-ts');
            }
            break;

          default:
            this._curKey = chunk.value;
        }
        break;

      case 'stringValue':
      case 'nullValue':
      case 'trueValue':
      case 'falseValue':
        switch (this._curState) {
          case 'tso-ts':
            this._curTimestamp = new Date(chunk.value);
            this._popState();
            break;

          default:
            this._curResult[this._curKey] = chunk.value;
        }
        break;

      case 'numberValue':
        this._curResult[this._curKey] = parseFloat(chunk.value);
        break;
    }

    callback();
  };
}

