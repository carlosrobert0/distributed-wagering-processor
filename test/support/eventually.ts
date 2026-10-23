export async function eventually(condition: () => boolean, options: { timeoutMs?: number; description?: string } = {}) {
  const deadline = Date.now() + (options.timeoutMs ?? 2_000);
  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  throw new Error(`Timed out waiting for ${options.description ?? 'condition'}`);
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
