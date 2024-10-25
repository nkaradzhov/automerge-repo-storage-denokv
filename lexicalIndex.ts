import { assertEquals } from '@std/assert/equals'

const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

export const getLexicalIndexAt = (i: number): string => {
    return (
        alphabet[alphabet.length - 1].repeat(Math.floor(i / alphabet.length)) +
        alphabet[i % alphabet.length]
    )
}

Deno.test('Lexical index', () => {
    const actual: string[] = []
    for (let i = 0; i < 1000; i++) {
        actual.push(getLexicalIndexAt(i))
    }

    assertEquals(actual, actual.concat().sort())
})
