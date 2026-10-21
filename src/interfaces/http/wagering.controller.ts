import { Body, Controller, Get, Headers, HttpStatus, Inject, Param, Post, Res } from '@nestjs/common';
import type { Response } from 'express';
import { InvalidPayloadError } from '../../application/errors';
import { ProcessWagerTransactionUseCase } from '../../application/wagering/process-wager-transaction.use-case';
import { WagerTransactionQueries } from '../../application/wagering/wager-transaction.queries';
import { WagerTransactionResult, WagerTransactionView } from '../../application/wagering/wager-transaction.views';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { WagerTransactionContract } from '../contracts/wager-transaction.contract';
import { currentCorrelationId } from './correlation-id.middleware';
import { parseBody } from './validation';

export function httpStatusFor(result: WagerTransactionResult): number {
  switch (result.status) {
    case WagerTransactionStatus.Processed:
      return result.idempotentReplay ? HttpStatus.OK : HttpStatus.CREATED;
    case WagerTransactionStatus.PendingReference:
    case WagerTransactionStatus.Pending:
      return HttpStatus.ACCEPTED;
    case WagerTransactionStatus.Rejected:
    case WagerTransactionStatus.Failed:
      return HttpStatus.UNPROCESSABLE_ENTITY;
  }
}

@Controller()
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransactionUseCase) private readonly processWager: ProcessWagerTransactionUseCase,
    @Inject(WagerTransactionQueries) private readonly queries: WagerTransactionQueries,
  ) {}

  @Post('wagering/transactions')
  async submit(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) response: Response,
  ): Promise<WagerTransactionResult> {
    if (!idempotencyKey || idempotencyKey.trim().length === 0) {
      throw new InvalidPayloadError('Idempotency-Key header is required', undefined, 'IDEMPOTENCY_KEY_REQUIRED');
    }
    const input = parseBody(WagerTransactionContract, body);
    const result = await this.processWager.execute(
      { ...input, idempotencyKey },
      { correlationId: currentCorrelationId(), source: 'http' },
    );
    response.status(httpStatusFor(result));
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  byId(@Param('transactionId') transactionId: string): Promise<WagerTransactionView> {
    return this.queries.byId(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  byExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionView> {
    return this.queries.byExternalId(providerId, externalTransactionId);
  }
}
