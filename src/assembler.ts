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

export interface Token {
  name: string;
  value?: any;
}

export type ObjectIndex = string | number;

export interface AssemblerOptions {
  onArrayPush?: (value: any, stack: any[], keyStack?: ObjectIndex[]) => boolean | void;
  onKeyValueAdd?: (key: ObjectIndex, value: any, stack?: any[], keyStack?: ObjectIndex[]) => boolean | void;
}

export class Assembler {
  public stack: any[] = [];
  public keyStack: ObjectIndex[] = [];
  public current: any = null;
  public key: ObjectIndex | null = null;

  private onArrayPush: (value: any, stack: any[], keyStack?: ObjectIndex[]) => boolean | void;
  private onKeyValueAdd: (key: ObjectIndex, value: any, stack?: any[], keyStack?: ObjectIndex[]) => boolean | void;

  constructor(options: AssemblerOptions = {}) {
    this.onArrayPush = options.onArrayPush;
    this.onKeyValueAdd = options.onKeyValueAdd;
  }

  private _pushStacks(newCurrent: any): void {
    if (this.current) this.keyStack.push(this.key);
    this.stack.push(this.current = newCurrent);
  }

  private _popStacks(): void {
    const stack = this.stack;
    stack.pop();
    this.current = stack[stack.length - 1] || null;
    this.key = this.keyStack.pop();
  }

  private _saveValue(value: any): void {
    const { current, key } = this;
    if (current) {
      if (Array.isArray(current)) {
        const { onArrayPush } = this;
        if (!onArrayPush || onArrayPush.call(this, value, this.stack, this.keyStack) !== false) {
          current.push(value);
          this.key = (key as number) + 1;
        }
      } else {
        const { onKeyValueAdd } = this;
        if (!onKeyValueAdd || onKeyValueAdd.call(this, key, value, this.stack, this.keyStack) !== false) {
          current[key] = value;
        }
        this.key = null;
      }
    } else {
      this.current = value;
    }
  }

  public process(token: Token): void {
    switch (token.name) {
      case 'startObject':
        this._pushStacks({});
        break;

      case 'startArray':
        this._pushStacks([]);
        this.key = 0;
        break;

      case 'endObject':
      case 'endArray':
        const finishedCurrent = this.current;
        this._popStacks();
        this._saveValue(finishedCurrent);
        break;

      case 'keyValue':
        this.key = token.value;
        break;

      case 'stringValue':
      case 'nullValue':
      case 'trueValue':
      case 'falseValue':
        this._saveValue(token.value);
        break;

      case 'numberValue':
        this._saveValue(parseFloat(token.value));
        break;

      default:
        break;
    }
  }
}
