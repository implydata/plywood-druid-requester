declare module 'socks5-http-client' {
  const p: any;
  export = p;
}

declare module 'socks5-http-client/lib/Agent' {
  import type { Agent } from 'http';

  const agent: Agent;
  export = agent;
}
