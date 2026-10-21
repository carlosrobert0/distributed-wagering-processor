import { v7 as uuidv7 } from 'uuid';
import { Clock, IdGenerator } from '../../application/ports/system';

export class SystemClock implements Clock {
  now(): Date {
    return new Date();
  }
}

export class UuidV7Generator implements IdGenerator {
  next(): string {
    return uuidv7();
  }
}
