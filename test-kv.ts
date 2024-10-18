import { testAll } from './all-tests.ts'
import { DenoKVStorageAdapter } from './kv-storage-adapter.ts'

testAll(new DenoKVStorageAdapter(await Deno.openKv('TEST_DATA')))
