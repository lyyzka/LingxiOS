function record(value: unknown, fields: string[]): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('questionnaire object required')
  if (Object.keys(value).some(key => !fields.includes(key))) throw new Error('unknown questionnaire field')
  return value as Record<string, unknown>
}

function text(value: unknown, max: number): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`questionnaire text must contain 1..${max} characters`)
  return value.trim()
}

export function questionnaire(args: Record<string, unknown>) {
  const title = args['title'] === undefined ? 'Agent 提问' : text(args['title'], 160)
  const rawItems = args['items']
  if (!Array.isArray(rawItems) || rawItems.length < 1 || rawItems.length > 8) throw new Error('items must contain 1..8 questions')
  const names = new Set<string>()
  const items = rawItems.map((raw, index) => {
    const item = record(raw, ['name', 'prompt', 'description', 'required', 'multiple', 'choices', 'input'])
    for (const flag of ['required', 'multiple']) {
      if (item[flag] !== undefined && typeof item[flag] !== 'boolean') throw new Error(`${flag} must be boolean`)
    }
    const name = item['name'] === undefined ? `question_${index + 1}` : text(item['name'], 64)
    if (!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(name) || names.has(name)) throw new Error('question names must be unique identifiers')
    names.add(name)
    const rawChoices = item['choices'] ?? []
    if (!Array.isArray(rawChoices) || rawChoices.length > 12) throw new Error('at most 12 choices per question')
    const values = new Set<string>()
    const choices = rawChoices.map(raw => {
      const choice = record(raw, ['value', 'label', 'description', 'disabled'])
      if (choice['disabled'] !== undefined && typeof choice['disabled'] !== 'boolean') throw new Error('disabled must be boolean')
      const value = text(choice['value'], 120)
      if (values.has(value)) throw new Error('duplicate choice value')
      values.add(value)
      return { value, label: text(choice['label'], 500),
        ...(choice['description'] === undefined ? {} : { description: text(choice['description'], 500) }),
        ...(choice['disabled'] === true ? { disabled: true } : {}),
      }
    })
    const input = item['input'] === undefined ? undefined : record(item['input'], ['label', 'placeholder'])
    if (!choices.length && !input) throw new Error('question requires choices or freeform input')
    return { name, prompt: text(item['prompt'], 500), choices,
      ...(item['description'] === undefined ? {} : { description: text(item['description'], 1000) }),
      ...(item['required'] === true ? { required: true } : {}), ...(item['multiple'] === true ? { multiple: true } : {}),
      ...(input ? { input: { label: text(input['label'], 120), ...(input['placeholder'] === undefined ? {} : { placeholder: text(input['placeholder'], 160) }) } } : {}),
    }
  })
  return { title, items, ...(args['submitLabel'] === undefined ? {} : { submitLabel: text(args['submitLabel'], 80) }) }
}
