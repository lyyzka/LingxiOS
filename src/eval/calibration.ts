type Verdict = 'meets_rubric' | 'does_not_meet' | 'uncertain'

/** Labels must be independently human-reviewed; this function does not certify their provenance. */
export function summarizeCalibration(samples: readonly { inputSha256: string; expected: Verdict; actual: Verdict }[]) {
  if (!Array.isArray(samples) || samples.length > 10_000) throw new Error('invalid calibration samples')
  const verdicts = ['meets_rubric', 'does_not_meet', 'uncertain']
  const seen = new Set<string>()
  let agreements = 0, falsePasses = 0, falseFailures = 0, uncertain = 0
  for (const sample of samples) {
    if (!sample || typeof sample.inputSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(sample.inputSha256)
      || !verdicts.includes(sample.expected) || !verdicts.includes(sample.actual)) throw new Error('invalid calibration sample')
    if (seen.has(sample.inputSha256)) throw new Error('duplicate calibration input; summarize repeated runs separately')
    seen.add(sample.inputSha256)
    if (sample.expected === sample.actual) agreements++
    if (sample.actual === 'meets_rubric' && sample.expected !== 'meets_rubric') falsePasses++
    if (sample.actual === 'does_not_meet' && sample.expected === 'meets_rubric') falseFailures++
    if (sample.actual === 'uncertain') uncertain++
  }
  return { sampleCount: samples.length, agreements, falsePasses, falseFailures, uncertain,
    agreementRate: samples.length ? agreements / samples.length : null,
    falsePassFraction: samples.length ? falsePasses / samples.length : null }
}
