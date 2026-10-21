import { LockMode } from '@mikro-orm/core';
import { EntityManager } from '@mikro-orm/postgresql';
import { ConcurrencyViolationError, WalletAlreadyExistsError } from '../../../application/errors';
import { WalletRepository } from '../../../application/ports/repositories';
import { Wallet } from '../../../domain/wallet/wallet';
import { isUniqueViolation } from '../database-errors';
import { WalletMapper } from '../mappers';
import { WalletRecord } from '../records';

/** Records são convertidos em domínio na hora; o identity map não agrega nada e poderia servir dado velho. */
const FRESH = { disableIdentityMap: true } as const;

export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<Wallet | null> {
    const record = await this.em.findOne(WalletRecord, { id }, FRESH);
    return record ? WalletMapper.toDomain(record) : null;
  }

  async findByIdForUpdate(id: string): Promise<Wallet | null> {
    // SELECT … FOR UPDATE: lock de LINHA, por wallet (sem lock global — restrição 6).
    const record = await this.em.findOne(WalletRecord, { id }, { ...FRESH, lockMode: LockMode.PESSIMISTIC_WRITE });
    return record ? WalletMapper.toDomain(record) : null;
  }

  async findByIdForShare(id: string): Promise<Wallet | null> {
    // SELECT … FOR SHARE: leitores convivem; escritores esperam.
    const record = await this.em.findOne(WalletRecord, { id }, { ...FRESH, lockMode: LockMode.PESSIMISTIC_READ });
    return record ? WalletMapper.toDomain(record) : null;
  }

  async insert(wallet: Wallet): Promise<void> {
    try {
      await this.em.insert(WalletRecord, WalletMapper.toRecord(wallet));
    } catch (error) {
      if (isUniqueViolation(error, 'wallets_player_currency_uq')) {
        throw new WalletAlreadyExistsError(wallet.playerId, wallet.currency);
      }
      throw error;
    }
  }

  async saveBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    // UPDATE condicionado à versão: defesa em profundidade sobre o FOR UPDATE.
    const affected = await this.em.nativeUpdate(
      WalletRecord,
      { id: wallet.id, version: expectedVersion },
      { balance: wallet.balance.toJSON().amount, version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) {
      throw new ConcurrencyViolationError(wallet.id, expectedVersion);
    }
  }
}
