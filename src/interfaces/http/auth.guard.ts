import { CanActivate, ExecutionContext, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';

export const IS_PUBLIC = 'isPublic';

export const Public = () => SetMetadata(IS_PUBLIC, true);

export interface ProviderAuthenticator {
  authenticate(authorization: string | undefined, providerId: string | undefined): Promise<boolean>;
}

export const PROVIDER_AUTHENTICATOR = Symbol('ProviderAuthenticator');

export class AllowAllProviderAuthenticator implements ProviderAuthenticator {
  async authenticate(): Promise<boolean> {
    return true;
  }
}

export class ProviderAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly authenticator: ProviderAuthenticator,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) {
      return true;
    }
    const request = context.switchToHttp().getRequest<Request>();
    const providerId = (request.body as { providerId?: unknown } | undefined)?.providerId;
    return this.authenticator.authenticate(
      request.headers.authorization,
      typeof providerId === 'string' ? providerId : undefined,
    );
  }
}
