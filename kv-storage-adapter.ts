import type {
    Chunk,
    StorageAdapterInterface,
    StorageKey
} from 'npm:@automerge/automerge-repo@1.2.1'
import { getLexicalIndexAt } from './lexicalIndex.ts'

type Data = Uint8Array
// Values have a maximum length of 64 KiB after serialization.
// https://docs.deno.com/api/deno/~/Deno.Kv
const VALUE_MAX_LEN = 65536

export class DenoKVStorageAdapter implements StorageAdapterInterface {
    constructor(private kv: Deno.Kv) {}

    async load(key: StorageKey): Promise<Data | undefined> {
        const entry = await this.kv.get<Data>(key)

        if (entry.value) return entry.value

        const list = this.kv.list<Data>({
            prefix: key
        })
        const returnData: number[] = []
        for await (const entry of list) {
            returnData.push(...entry.value)
        }

        if (returnData.length === 0) return undefined

        return new Uint8Array(returnData)
    }

    async save(key: StorageKey, data: Data): Promise<void> {
        if (data.length > VALUE_MAX_LEN) {
            /**
             * Threre might be a "single" value for this key,
             * so clear it out
             */
            await this.kv.delete(key)

            /**
             * Split the value into chunks and save them with a `chunk key`
             *
             * The `chunk key` is constructed by suffixing the original key
             * with the lexically ordered index by chunk number:
             *
             * chunk 0  -> ['original', 'key', 'a']
             * chunk 1  -> ['original', 'key', 'b']
             * ...
             * chunk 25 -> ['original', 'key', 'z']
             * chunk 26 -> ['original', 'key', 'za']
             * chunk 27 -> ['original', 'key', 'zb']
             * ...
             * chunk 51 -> ['original', 'key', 'zz']
             * chunk 52 -> ['original', 'key', 'zza']
             * chunk 53 -> ['original', 'key', 'zzb']
             * ...
             * chunk 77 -> ['original', 'key', 'zzz']
             * ...
             */
            const promises: Promise<void>[] = []
            let chunkNumber = 0
            for (let i = 0; i < data.length; i = i + VALUE_MAX_LEN) {
                const chunkKey = key.concat(getLexicalIndexAt(chunkNumber++))
                const sliced = data.slice(
                    i,
                    Math.min(i + VALUE_MAX_LEN, data.length)
                )

                this.kv.set(chunkKey, sliced)
            }
            await Promise.all(promises)
        } else {
            /**
             * There might be chunked values for this key, so clear them out
             */
            const list = await this.kv.list<Data>({
                prefix: key
            })

            const promises = []
            for await (const entry of list) {
                promises.push(this.kv.delete(entry.key))
            }
            await Promise.all(promises)
            //

            await this.kv.set(key, data)
        }
    }

    async remove(key: StorageKey) {
        const list = await this.kv.list<Data>({
            prefix: key
        })
        const promises = []
        for await (const entry of list) {
            promises.push(this.kv.delete(entry.key))
        }
        await Promise.all(promises)
        await this.kv.delete(key)
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
}
