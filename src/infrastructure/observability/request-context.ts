import { AsyncLocalStorage } from 'node:async_hooks';

/** Contexto de correlação propagado por toda a cadeia assíncrona de uma requisição/mensagem. */
export interface RequestContext {
  correlationId: string;
  messageId?: string;
}

const storage = new AsyncLocalStorage<RequestContext>();

export const RequestContextStore = {
  run<T>(context: RequestContext, fn: () => T): T {
    return storage.run(context, fn);
  },

  current(): RequestContext | undefined {
    return storage.getStore();
  },
};
