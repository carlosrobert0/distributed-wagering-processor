import { afterAll, setDefaultTimeout } from 'bun:test';
import { closeTestPools } from './database';

setDefaultTimeout(120_000);

afterAll(() => closeTestPools());
