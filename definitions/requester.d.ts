declare module Requester {
  interface DatabaseRequest<T> {
    query: T;
    context?: { [key: string]: any };
  }

  interface FacetRequester<T> {
    (request: DatabaseRequest<T>): Q.Promise<any>;
  }
}
