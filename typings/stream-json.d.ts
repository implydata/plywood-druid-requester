declare module 'stream-json' {
  const p: any;
  export = p;
}

declare module 'stream-json/Combo' {
  import { Transform } from 'stream';

  namespace Combo {}
  class Combo extends Transform {
    constructor(options: any);
  }

  export = Combo;
}
