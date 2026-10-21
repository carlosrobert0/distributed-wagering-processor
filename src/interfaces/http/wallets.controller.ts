import { Body, Controller, Get, HttpCode, HttpStatus, Inject, Param, Post, Query } from '@nestjs/common';
import { InvalidPayloadError } from '../../application/errors';
import { CreateWalletUseCase } from '../../application/wallets/create-wallet.use-case';
import { ReconcileWalletUseCase } from '../../application/wallets/reconcile-wallet.use-case';
import { WalletQueries } from '../../application/wallets/wallet.queries';
import { LedgerPageView, ReconciliationView, WalletView } from '../../application/wallets/wallet.views';
import { CreateWalletContract } from '../contracts/wager-transaction.contract';
import { currentCorrelationId } from './correlation-id.middleware';
import { parseBody } from './validation';

@Controller('wallets')
export class WalletsController {
  constructor(
    @Inject(CreateWalletUseCase) private readonly createWallet: CreateWalletUseCase,
    @Inject(WalletQueries) private readonly queries: WalletQueries,
    @Inject(ReconcileWalletUseCase) private readonly reconcile: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(@Body() body: unknown): Promise<Pick<WalletView, 'id' | 'playerId' | 'balance' | 'version'>> {
    const input = parseBody(CreateWalletContract, body);
    const wallet = await this.createWallet.execute(input, { correlationId: currentCorrelationId() });
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance, version: wallet.version };
  }

  @Get(':walletId')
  byId(@Param('walletId') walletId: string): Promise<WalletView> {
    return this.queries.byId(walletId);
  }

  @Get(':walletId/ledger')
  ledger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ): Promise<LedgerPageView> {
    return this.queries.ledger(walletId, { cursor, limit: parseLimit(limit) });
  }

  @Post(':walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  reconciliation(@Param('walletId') walletId: string): Promise<ReconciliationView> {
    return this.reconcile.execute(walletId, { correlationId: currentCorrelationId() });
  }
}

function parseLimit(limit: string | undefined): number | undefined {
  if (limit === undefined) return undefined;
  if (!/^\d{1,4}$/.test(limit)) {
    throw new InvalidPayloadError('limit must be a positive integer');
  }
  return Number(limit);
}
