import { describe, expect, mock, test } from 'bun:test';
import { ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import {
  AllowAllProviderAuthenticator,
  ProviderAuthenticator,
  ProviderAuthGuard,
} from '../../../src/interfaces/http/auth.guard';

function contextFor(request: { headers: Record<string, string>; body?: unknown }): ExecutionContext {
  return {
    getHandler: () => () => undefined,
    getClass: () => class {},
    switchToHttp: () => ({ getRequest: () => request }),
  } as unknown as ExecutionContext;
}

function reflectorReturning(isPublic: boolean): Reflector {
  return { getAllAndOverride: () => isPublic } as unknown as Reflector;
}

describe('ProviderAuthGuard', () => {
  test('rotas públicas não consultam o autenticador', async () => {
    const authenticate = mock(async () => false);
    const guard = new ProviderAuthGuard(reflectorReturning(true), { authenticate });
    expect(await guard.canActivate(contextFor({ headers: {} }))).toBe(true);
    expect(authenticate).not.toHaveBeenCalled();
  });

  test('rotas protegidas delegam ao autenticador com o token e o providerId do payload', async () => {
    const authenticator: ProviderAuthenticator = { authenticate: mock(async () => false) };
    const guard = new ProviderAuthGuard(reflectorReturning(false), authenticator);
    const allowed = await guard.canActivate(
      contextFor({ headers: { authorization: 'Bearer token' }, body: { providerId: 'provider-a' } }),
    );
    expect(allowed).toBe(false);
    expect(authenticator.authenticate).toHaveBeenCalledWith('Bearer token', 'provider-a');
  });

  test('implementação padrão (autenticação fora do escopo) permite tudo', async () => {
    const guard = new ProviderAuthGuard(reflectorReturning(false), new AllowAllProviderAuthenticator());
    expect(await guard.canActivate(contextFor({ headers: {} }))).toBe(true);
  });
});
