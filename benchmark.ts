import { DenoKVStorageAdapter } from './mod.ts'

const kv = await Deno.openKv('BENCH_DATA')
const adapter = new DenoKVStorageAdapter(kv)

const LARGE_PAYLOAD = new Uint8Array(3000000).map(() => Math.random() * 256)

adapter.save(['loadtest'], LARGE_PAYLOAD)

Deno.bench('load', async () => {
    const data = await adapter.load(['loadtest'])
})
