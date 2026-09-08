/** Only the top-level JSON candidate's body is public. Plain/malformed/unknown protocols fail closed. */
export class CandidateBodyParser {
  private state: 'start' | 'key' | 'colon' | 'value' | 'body' | 'skip' | 'separator' | 'done' | 'invalid' = 'start'
  private token = ''
  private key = ''
  private quoted = false
  private escaped = false
  private depth = 0
  private size = 0
  private readonly keys = new Set<string>()
  private escape = ''
  private highSurrogate = ''

  get invalid(): boolean { return this.state === 'invalid' }
  get complete(): boolean { return this.state === 'done' && this.keys.has('body') }

  push(chunk: string): string {
    this.size += chunk.length
    if (this.size > 256_000) { this.state = 'invalid'; return '' }
    let output = ''
    const append = (text: string) => {
      for (const char of text) {
        if (this.highSurrogate) { output += this.highSurrogate + char; this.highSurrogate = '' }
        else if (char.length === 1 && char.charCodeAt(0) >= 0xd800 && char.charCodeAt(0) <= 0xdbff) this.highSurrogate = char
        else output += char
      }
    }
    try {
      for (const char of chunk) {
        if (this.state === 'invalid') break
        if (this.state === 'body') {
          if (this.escape) {
            this.escape += char
            if (this.escape === '\\u' || this.escape.startsWith('\\u') && this.escape.length < 6) continue
            append(JSON.parse('"' + this.escape + '"') as string)
            this.escape = ''
          } else if (char === '\\') this.escape = char
          else if (char === '"') {
            if (this.highSurrogate) throw new Error('unfinished surrogate')
            this.state = 'separator'
          } else {
            if (char.charCodeAt(0) < 32) throw new Error('unescaped control')
            append(char)
          }
          continue
        }
        if (this.state === 'key') {
          if (!this.token && /\s/.test(char)) continue
          if (!this.token && char !== '"') throw new Error('expected key')
          this.token += char
          if (this.token.length === 1) continue
          if (this.escaped) { this.escaped = false; continue }
          if (char === '\\') { this.escaped = true; continue }
          if (char !== '"') continue
          this.key = JSON.parse(this.token) as string
          if (!['body','status','checks','gaps','taskRef'].includes(this.key) || this.keys.has(this.key)) throw new Error('unknown or repeated key')
          this.keys.add(this.key)
          this.token = ''
          this.state = 'colon'
          continue
        }
        if (this.state === 'skip') {
          if (!this.quoted && this.depth === 0 && (char === ',' || char === '}')) {
            JSON.parse(this.token)
            this.token = ''
            this.state = char === ',' ? 'key' : 'done'
            continue
          }
          this.token += char
          if (this.quoted) {
            if (this.escaped) this.escaped = false
            else if (char === '\\') this.escaped = true
            else if (char === '"') this.quoted = false
          } else if (char === '"') this.quoted = true
          else if (char === '{' || char === '[') this.depth++
          else if (char === '}' || char === ']') this.depth--
          if (this.depth < 0) throw new Error('invalid nesting')
          continue
        }
        if (/\s/.test(char)) continue
        if (this.state === 'start' && char === '{') this.state = 'key'
        else if (this.state === 'colon' && char === ':') this.state = 'value'
        else if (this.state === 'value') {
          if (this.key === 'body') {
            if (char !== '"') throw new Error('body must be text')
            this.state = 'body'
          } else {
            this.token = char
            this.quoted = char === '"'
            this.depth = char === '{' || char === '[' ? 1 : 0
            this.state = 'skip'
          }
        } else if (this.state === 'separator' && char === ',') this.state = 'key'
        else if (this.state === 'separator' && char === '}') this.state = 'done'
        else throw new Error('invalid candidate JSON')
      }
    } catch { this.state = 'invalid'; return '' }
    return output
  }
}
