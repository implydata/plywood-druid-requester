declare module 'socks5-https-client' {
  const p: any;
  export = p;
}

declare module 'socks5-https-client/lib/Agent' {
  import type { Agent } from 'http';

  const agent: Agent;
  export = agent;
}
