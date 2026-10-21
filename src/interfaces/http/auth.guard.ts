import { CanActivate, ExecutionContext, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

export const IS_PUBLIC = 'isPublic';

/** Marca rotas abertas (health, métricas) — continuam abertas quando a autenticação for ligada. */
export const Public = () => SetMetadata(IS_PUBLIC, true);

/**
 * Ponto de extensão de autenticação (§2) — hoje NO-OP, por decisão documentada no ARCHITECTURE.md.
 *
 * Desenho previsto (Keycloak, OIDC):
 *  1. cada provedor é um client confidencial no realm `wagering` e obtém um access token via
 *     client credentials;
 *  2. este guard valida o JWT (assinatura via JWKS em cache, `iss`, `aud`, `exp`);
 *  3. o `providerId` vem de uma claim do token e é comparado com `body.providerId` — divergência
 *     → 403. Assim um provedor não consegue movimentar transações de outro.
 * Rotas `@Public()` (health, métricas) seguem abertas. Mensagens da fila são canal interno.
 */
export class ProviderAuthGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) {
      return true;
    }
    // NO-OP: autenticação não implementada neste desafio (vale 0 pontos — ver ARCHITECTURE.md).
    return true;
  }
}
