import {
    Chunk,
    StorageAdapterInterface,
    type StorageKey
} from 'npm:@automerge/automerge-repo@1.2.1'

type Data = Uint8Array

export class DenoKVStorageAdapter implements StorageAdapterInterface {
    constructor(private kv: Deno.Kv) {}

    async load(key: StorageKey): Promise<Data | undefined> {
        const entry = await this.kv.get<Data>(key)
        return entry.value ?? undefined
    }

    async save(key: StorageKey, data: Data): Promise<void> {
        await this.kv.set(key, data)
    }

    remove(key: StorageKey): Promise<void> {
        return this.kv.delete(key)
    }

    async loadRange(keyPrefix: StorageKey): Promise<Chunk[]> {
        const list = await this.kv.list<Data>({
            prefix: keyPrefix
        })

        const range: Chunk[] = []

        for await (const entry of list) {
            range.push({
                key: entry.key.map(String),
                data: entry.value
            })
        }
        return range
    }

    async removeRange(keyPrefix: StorageKey): Promise<void> {
        const list = await this.kv.list<Data>({
            prefix: keyPrefix
        })

        for await (const entry of list) {
            this.kv.delete(entry.key)
        }
    }

    async clearAll() {
        const list = this.kv.list({ prefix: [] })
        const promises = []
        for await (const entry of list) {
            promises.push(this.kv.delete(entry.key))
        }
        await Promise.all(promises)
    }
}
