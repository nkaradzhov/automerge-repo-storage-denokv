import { assertEquals, assertNotEquals } from 'jsr:@std/assert'

type StorageKey = string[]
type Chunk = {
    key: StorageKey
    data: Uint8Array | undefined
}
interface StorageAdapterInterface {
    load(key: StorageKey): Promise<Uint8Array | undefined>
    save(key: StorageKey, data: Uint8Array): Promise<void>
    remove(key: StorageKey): Promise<void>
    loadRange(keyPrefix: StorageKey): Promise<Chunk[]>
    removeRange(keyPrefix: StorageKey): Promise<void>
}

const PAYLOAD_A = () => new Uint8Array([0, 1, 127, 99, 154, 235])
const PAYLOAD_B = () => new Uint8Array([1, 76, 160, 53, 57, 10, 230])
const PAYLOAD_C = () => new Uint8Array([2, 111, 74, 131, 236, 96, 142, 193])

const LARGE_PAYLOAD = new Uint8Array(100_000).map(() => Math.random() * 256)

type CleanupFunction = () => Promise<void>

export function testAll(
    adapter: StorageAdapterInterface,
    cleanup: CleanupFunction
) {
    Deno.test(`Storage adapter acceptance tests`, async t => {
        await cleanup()

        await t.step('load', async t => {
            await t.step(
                'should return undefined if there is no data',
                async t => {
                    const actual = await adapter.load([
                        'AAAAA',
                        'sync-state',
                        'xxxxx'
                    ])
                    assertEquals(actual, undefined)
                }
            )
        })

        await t.step('save and load', async t => {
            await t.step('should return data that was saved', async t => {
                await adapter.save(['storage-adapter-id'], PAYLOAD_A())
                const actual = await adapter.load(['storage-adapter-id'])
                assertEquals(actual, PAYLOAD_A())
            })

            await t.step('should work with composite keys', async t => {
                await adapter.save(
                    ['AAAAA', 'sync-state', 'xxxxx'],
                    PAYLOAD_A()
                )
                const actual = await adapter.load([
                    'AAAAA',
                    'sync-state',
                    'xxxxx'
                ])
                assertEquals(actual, PAYLOAD_A())
            })

            await t.step('should work with a large payload', async t => {
                await cleanup()
                try {
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        LARGE_PAYLOAD
                    )
                } catch (e) {
                    assertEquals(true, false, e as string)
                }

                const actual = await adapter.load([
                    'AAAAA',
                    'sync-state',
                    'xxxxx'
                ])

                assertEquals(actual, LARGE_PAYLOAD)
            })
        })

        await t.step('loadRange', async t => {
            await cleanup()
            await t.step(
                'should return an empty array if there is no data',
                async () => {
                    assertEquals(await adapter.loadRange(['AAAAA']), [])
                }
            )
        })

        await t.step('save and loadRange', async t => {
            await t.step(
                'should return all the data that matches the key',
                async t => {
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        PAYLOAD_A()
                    )
                    await adapter.save(
                        ['AAAAA', 'snapshot', 'yyyyy'],
                        PAYLOAD_B()
                    )
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'zzzzz'],
                        PAYLOAD_C()
                    )

                    const sortfn = (
                        a: { key: string[] },
                        b: { key: string[] }
                    ) => a.key.join().localeCompare(b.key.join())

                    assertEquals(
                        (await adapter.loadRange(['AAAAA'])).sort(sortfn),
                        [
                            {
                                key: ['AAAAA', 'sync-state', 'xxxxx'],
                                data: PAYLOAD_A()
                            },
                            {
                                key: ['AAAAA', 'snapshot', 'yyyyy'],
                                data: PAYLOAD_B()
                            },
                            {
                                key: ['AAAAA', 'sync-state', 'zzzzz'],
                                data: PAYLOAD_C()
                            }
                        ].sort(sortfn)
                    )

                    assertEquals(
                        await adapter.loadRange(['AAAAA', 'sync-state']),
                        [
                            {
                                key: ['AAAAA', 'sync-state', 'xxxxx'],
                                data: PAYLOAD_A()
                            },
                            {
                                key: ['AAAAA', 'sync-state', 'zzzzz'],
                                data: PAYLOAD_C()
                            }
                        ]
                    )
                }
            )

            await t.step(
                'should only load values that match they key',
                async t => {
                    await cleanup()
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        PAYLOAD_A()
                    )
                    await adapter.save(
                        ['BBBBB', 'sync-state', 'zzzzz'],
                        PAYLOAD_C()
                    )

                    const actual = await adapter.loadRange(['AAAAA'])

                    assertEquals(actual, [
                        {
                            key: ['AAAAA', 'sync-state', 'xxxxx'],
                            data: PAYLOAD_A()
                        }
                    ])
                    assertNotEquals(actual, [
                        {
                            key: ['BBBBB', 'sync-state', 'zzzzz'],
                            data: PAYLOAD_C()
                        }
                    ])
                }
            )
        })

        await t.step('save and remove', async t => {
            await t.step('after removing, should be empty', async t => {
                await cleanup()
                await adapter.save(['AAAAA', 'snapshot', 'xxxxx'], PAYLOAD_A())
                await adapter.remove(['AAAAA', 'snapshot', 'xxxxx'])

                assertEquals(await adapter.loadRange(['AAAAA']), [])
                assertEquals(
                    await adapter.load(['AAAAA', 'snapshot', 'xxxxx']),
                    undefined
                )
            })
        })

        await t.step('save and save', async t => {
            await t.step(
                'should overwrite data saved with the same key',
                async () => {
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        PAYLOAD_A()
                    )
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        PAYLOAD_B()
                    )

                    assertEquals(
                        await adapter.loadRange(['AAAAA', 'sync-state']),
                        [
                            {
                                key: ['AAAAA', 'sync-state', 'xxxxx'],
                                data: PAYLOAD_B()
                            }
                        ]
                    )
                }
            )
        })

        await t.step('removeRange', async t => {
            await t.step('should remove a range of records', async t => {
                await adapter.save(
                    ['AAAAA', 'sync-state', 'xxxxx'],
                    PAYLOAD_A()
                )
                await adapter.save(['AAAAA', 'snapshot', 'yyyyy'], PAYLOAD_B())
                await adapter.save(
                    ['AAAAA', 'sync-state', 'zzzzz'],
                    PAYLOAD_C()
                )

                await adapter.removeRange(['AAAAA', 'sync-state'])

                assertEquals(await adapter.loadRange(['AAAAA']), [
                    {
                        key: ['AAAAA', 'snapshot', 'yyyyy'],
                        data: PAYLOAD_B()
                    }
                ])
            })

            await t.step(
                "should not remove records that don't match",
                async t => {
                    await adapter.save(
                        ['AAAAA', 'sync-state', 'xxxxx'],
                        PAYLOAD_A()
                    )
                    await adapter.save(
                        ['BBBBB', 'sync-state', 'zzzzz'],
                        PAYLOAD_B()
                    )

                    await adapter.removeRange(['AAAAA'])

                    const actual = await adapter.loadRange(['BBBBB'])
                    assertEquals(actual, [
                        {
                            key: ['BBBBB', 'sync-state', 'zzzzz'],
                            data: PAYLOAD_B()
                        }
                    ])
                }
            )
        })
    })
}
