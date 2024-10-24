import { testAll } from './all-tests.ts'
import { DenoKVStorageAdapter } from './kv-storage-adapter.ts'

const kv = await Deno.openKv('TEST_DATA')

const cleanup = async () => {
    const list = kv.list({ prefix: [] })
    const promises = []
    for await (const entry of list) {
        promises.push(kv.delete(entry.key))
    }
    await Promise.all(promises)
}

testAll(new DenoKVStorageAdapter(kv), cleanup)
