import { fileURLToPath } from 'node:url'
import mammoth from 'mammoth'

// A separate process keeps parser failures out of the request-serving process.
try {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of process.stdin) {
    size += chunk.length
    if (size > 16 * 1024 * 1024) throw new Error('Document input exceeds 16 MiB')
    chunks.push(Buffer.from(chunk))
  }
  const bytes = Buffer.concat(chunks)
  let text: string
  if (process.argv[2] === 'docx') {
    const result = await mammoth.extractRawText({ buffer: bytes })
    if (result.value.length > 990_000) throw new Error('extracted text exceeds limit')
    const warnings = result.messages.slice(0, 10).map(message => message.message.slice(0, 300))
    text = '[DOCX text extraction: formatting and images are not represented.'
      + (warnings.length ? ' Parser notices: ' + JSON.stringify(warnings) : '') + ']\n' + result.value
  } else if (process.argv[2] === 'pdf') {
    const { getDocument } = await import('pdfjs-dist/legacy/build/pdf.mjs')
    const resources = import.meta.resolve('pdfjs-dist/package.json')
    const task = getDocument({ data: new Uint8Array(bytes), disableFontFace: true, useSystemFonts: false, verbosity: 0,
      cMapUrl: fileURLToPath(new URL('./cmaps/', resources)).replaceAll('\\', '/'), cMapPacked: true,
      standardFontDataUrl: fileURLToPath(new URL('./standard_fonts/', resources)).replaceAll('\\', '/'),
      wasmUrl: fileURLToPath(new URL('./wasm/', resources)).replaceAll('\\', '/'),
    })
    try {
      const pdf = await task.promise
      if (pdf.numPages > 200) throw new Error('PDF exceeds 200 pages')
      text = '[PDF text extraction: page text only; images, layout and OCR are not represented.]\n'
      for (let number = 1; number <= pdf.numPages; number++) {
        const page = await pdf.getPage(number)
        try {
          const content = await page.getTextContent()
          const body = content.items.map(item => 'str' in item ? item.str + (item.hasEOL ? '\n' : ' ') : '').join('')
          text += `\n[Page ${number}]\n` + (body.trim() || '[No extractable text; OCR was not performed.]')
          if (text.length > 990_000) throw new Error('extracted text exceeds limit')
        } finally { page.cleanup() }
      }
    } finally { await task.destroy() }
  } else throw new Error('unsupported document format')
  process.stdout.write(text)

} catch {
  process.stderr.write('Document text extraction failed')
  process.exitCode = 1
}
