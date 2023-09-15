/* eslint-disable @typescript-eslint/array-type */
/* eslint-disable @typescript-eslint/unified-signatures */
/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable max-classes-per-file */
declare module 'readable-stream' {
  export class EventEmitter {
    addListener(event: string | symbol, listener: (...args: any[]) => void): this;
    on(event: string | symbol, listener: (...args: any[]) => void): this;
    once(event: string | symbol, listener: (...args: any[]) => void): this;
    removeListener(event: string | symbol, listener: (...args: any[]) => void): this;
    off(event: string | symbol, listener: (...args: any[]) => void): this;
    removeAllListeners(event?: string | symbol): this;
    setMaxListeners(n: number): this;
    getMaxListeners(): number;
    listeners(event: string | symbol): Function[];
    rawListeners(event: string | symbol): Function[];
    emit(event: string | symbol, ...args: any[]): boolean;
    listenerCount(type: string | symbol): number;
    // Added in Node 6...
    prependListener(event: string | symbol, listener: (...args: any[]) => void): this;
    prependOnceListener(event: string | symbol, listener: (...args: any[]) => void): this;
    eventNames(): Array<string | symbol>;
  }

  export interface ReadableStream extends EventEmitter {
    readable: boolean;
    isTTY?: boolean;
    read(size?: number): any;
    setEncoding(encoding: string | null): void;
    pause(): ReadableStream;
    resume(): ReadableStream;
    isPaused(): boolean;
    pipe<T extends WritableStream>(destination: T, options?: { end?: boolean }): T;
    unpipe<T extends WritableStream>(destination?: T): void;
    unshift(chunk: any): void;
    wrap(oldStream: ReadableStream): ReadableStream;
  }

  export interface WritableStream extends EventEmitter {
    writable: boolean;
    isTTY?: boolean;
    write(buffer: any, cb?: Function): boolean;
    write(str: string, encoding?: string, cb?: Function): boolean;
    end(): void;
    end(str: any, cb?: Function): void;
    end(str: string, encoding?: string, cb?: Function): void;
  }

  export interface ReadWriteStream extends ReadableStream, WritableStream {
    pause(): ReadWriteStream;
    resume(): ReadWriteStream;
  }

  // --------------

  export class Stream extends EventEmitter {
    pipe<T extends WritableStream>(destination: T, options?: { end?: boolean }): T;
  }

  export interface ReadableOptions {
    highWaterMark?: number;
    encoding?: string;
    objectMode?: boolean;
    read?: (size?: number) => any;
  }

  export class Readable extends EventEmitter implements ReadableStream {
    readable: boolean;
    constructor(opts?: ReadableOptions);
    protected _read(size: number): void;
    read(size?: number): any;
    setEncoding(encoding: string): void;
    pause(): Readable;
    resume(): Readable;
    isPaused(): boolean;
    pipe<T extends WritableStream>(destination: T, options?: { end?: boolean }): T;
    unpipe<T extends WritableStream>(destination?: T): void;
    unshift(chunk: any): void;
    wrap(oldStream: ReadableStream): ReadableStream;
    push(chunk: any, encoding?: string): boolean;

    /**
     * Event emitter
     * The defined events on documents including:
     *   1. close
     *   2. data
     *   3. end
     *   4. readable
     *   5. error
     **/
    addListener(event: string, listener: Function): this;
    addListener(event: string, listener: Function): this;
    addListener(event: 'close', listener: () => void): this;
    addListener(event: 'data', listener: (chunk: any) => void): this;
    addListener(event: 'end', listener: () => void): this;
    addListener(event: 'readable', listener: () => void): this;
    addListener(event: 'error', listener: (err: Error) => void): this;

    emit(event: string, ...args: any[]): boolean;
    emit(event: 'close'): boolean;
    emit(event: 'data', chunk: any): boolean;
    emit(event: 'end'): boolean;
    emit(event: 'readable'): boolean;
    emit(event: 'error', err: Error): boolean;

    on(event: string, listener: Function): this;
    on(event: 'close', listener: () => void): this;
    on(event: 'data', listener: (chunk: any) => void): this;
    on(event: 'end', listener: () => void): this;
    on(event: 'readable', listener: () => void): this;
    on(event: 'error', listener: (err: Error) => void): this;

    once(event: string, listener: Function): this;
    once(event: 'close', listener: () => void): this;
    once(event: 'data', listener: (chunk: any) => void): this;
    once(event: 'end', listener: () => void): this;
    once(event: 'readable', listener: () => void): this;
    once(event: 'error', listener: (err: Error) => void): this;

    prependListener(event: string, listener: Function): this;
    prependListener(event: 'close', listener: () => void): this;
    prependListener(event: 'data', listener: (chunk: any) => void): this;
    prependListener(event: 'end', listener: () => void): this;
    prependListener(event: 'readable', listener: () => void): this;
    prependListener(event: 'error', listener: (err: Error) => void): this;

    prependOnceListener(event: string, listener: Function): this;
    prependOnceListener(event: 'close', listener: () => void): this;
    prependOnceListener(event: 'data', listener: (chunk: any) => void): this;
    prependOnceListener(event: 'end', listener: () => void): this;
    prependOnceListener(event: 'readable', listener: () => void): this;
    prependOnceListener(event: 'error', listener: (err: Error) => void): this;

    removeListener(event: string, listener: Function): this;
    removeListener(event: 'close', listener: () => void): this;
    removeListener(event: 'data', listener: (chunk: any) => void): this;
    removeListener(event: 'end', listener: () => void): this;
    removeListener(event: 'readable', listener: () => void): this;
    removeListener(event: 'error', listener: (err: Error) => void): this;
  }

  export interface WritableOptions {
    highWaterMark?: number;
    decodeStrings?: boolean;
    objectMode?: boolean;
    write?: (chunk: any, encoding: string, callback: Function) => any;
    writev?: (chunks: { chunk: any; encoding: string }[], callback: Function) => any;
  }

  export class Writable extends EventEmitter implements WritableStream {
    writable: boolean;
    constructor(opts?: WritableOptions);
    protected _write(chunk: any, encoding: string, callback: Function): void;
    write(chunk: any, cb?: Function): boolean;
    write(chunk: any, encoding?: string, cb?: Function): boolean;
    end(): void;
    end(chunk: any, cb?: Function): void;
    end(chunk: any, encoding?: string, cb?: Function): void;

    /**
     * Event emitter
     * The defined events on documents including:
     *   1. close
     *   2. drain
     *   3. error
     *   4. finish
     *   5. pipe
     *   6. unpipe
     **/
    addListener(event: string, listener: Function): this;
    addListener(event: 'close', listener: () => void): this;
    addListener(event: 'drain', listener: () => void): this;
    addListener(event: 'error', listener: (err: Error) => void): this;
    addListener(event: 'finish', listener: () => void): this;
    addListener(event: 'pipe', listener: (src: Readable) => void): this;
    addListener(event: 'unpipe', listener: (src: Readable) => void): this;

    emit(event: string, ...args: any[]): boolean;
    emit(event: 'close'): boolean;
    emit(event: 'drain', chunk: any): boolean;
    emit(event: 'error', err: Error): boolean;
    emit(event: 'finish'): boolean;
    emit(event: 'pipe', src: Readable): boolean;
    emit(event: 'unpipe', src: Readable): boolean;

    on(event: string, listener: Function): this;
    on(event: 'close', listener: () => void): this;
    on(event: 'drain', listener: () => void): this;
    on(event: 'error', listener: (err: Error) => void): this;
    on(event: 'finish', listener: () => void): this;
    on(event: 'pipe', listener: (src: Readable) => void): this;
    on(event: 'unpipe', listener: (src: Readable) => void): this;

    once(event: string, listener: Function): this;
    once(event: 'close', listener: () => void): this;
    once(event: 'drain', listener: () => void): this;
    once(event: 'error', listener: (err: Error) => void): this;
    once(event: 'finish', listener: () => void): this;
    once(event: 'pipe', listener: (src: Readable) => void): this;
    once(event: 'unpipe', listener: (src: Readable) => void): this;

    prependListener(event: string, listener: Function): this;
    prependListener(event: 'close', listener: () => void): this;
    prependListener(event: 'drain', listener: () => void): this;
    prependListener(event: 'error', listener: (err: Error) => void): this;
    prependListener(event: 'finish', listener: () => void): this;
    prependListener(event: 'pipe', listener: (src: Readable) => void): this;
    prependListener(event: 'unpipe', listener: (src: Readable) => void): this;

    prependOnceListener(event: string, listener: Function): this;
    prependOnceListener(event: 'close', listener: () => void): this;
    prependOnceListener(event: 'drain', listener: () => void): this;
    prependOnceListener(event: 'error', listener: (err: Error) => void): this;
    prependOnceListener(event: 'finish', listener: () => void): this;
    prependOnceListener(event: 'pipe', listener: (src: Readable) => void): this;
    prependOnceListener(event: 'unpipe', listener: (src: Readable) => void): this;

    removeListener(event: string, listener: Function): this;
    removeListener(event: 'close', listener: () => void): this;
    removeListener(event: 'drain', listener: () => void): this;
    removeListener(event: 'error', listener: (err: Error) => void): this;
    removeListener(event: 'finish', listener: () => void): this;
    removeListener(event: 'pipe', listener: (src: Readable) => void): this;
    removeListener(event: 'unpipe', listener: (src: Readable) => void): this;
  }

  export interface DuplexOptions extends ReadableOptions, WritableOptions {
    allowHalfOpen?: boolean;
    readableObjectMode?: boolean;
    writableObjectMode?: boolean;
  }

  // Note: Duplex extends both Readable and Writable.
  export class Duplex extends Readable implements ReadWriteStream {
    // Readable
    pause(): Duplex;
    resume(): Duplex;
    // Writeable
    writable: boolean;
    constructor(opts?: DuplexOptions);
    protected _write(chunk: any, encoding: string, callback: Function): void;
    write(chunk: any, cb?: Function): boolean;
    write(chunk: any, encoding?: string, cb?: Function): boolean;
    end(): void;
    end(chunk: any, cb?: Function): void;
    end(chunk: any, encoding?: string, cb?: Function): void;
  }

  export interface TransformOptions extends DuplexOptions {
    transform?: (chunk: any, encoding: string, callback: Function) => any;
    flush?: (callback: Function) => any;
  }

  // Note: Transform lacks the _read and _write methods of Readable/Writable.
  export class Transform extends EventEmitter implements ReadWriteStream {
    readable: boolean;
    writable: boolean;
    constructor(opts?: TransformOptions);
    protected _transform(chunk: any, encoding: string, callback: Function): void;
    protected _flush(callback: Function): void;
    read(size?: number): any;
    setEncoding(encoding: string): void;
    pause(): Transform;
    resume(): Transform;
    isPaused(): boolean;
    pipe<T extends WritableStream>(destination: T, options?: { end?: boolean }): T;
    unpipe<T extends WritableStream>(destination?: T): void;
    unshift(chunk: any): void;
    wrap(oldStream: ReadableStream): ReadableStream;
    push(chunk: any, encoding?: string): boolean;
    write(chunk: any, cb?: Function): boolean;
    write(chunk: any, encoding?: string, cb?: Function): boolean;
    end(): void;
    end(chunk: any, cb?: Function): void;
    end(chunk: any, encoding?: string, cb?: Function): void;
  }

  export class PassThrough extends Transform {}
}
