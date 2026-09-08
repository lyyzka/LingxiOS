import type { ModelDriver } from './driver.js'
import { ResourceQuota } from '../resource-quota.js'

export function limitModel(source: ModelDriver, quota: ResourceQuota): ModelDriver {
  return new Proxy(source, { get(target, property) {
    if (property === 'singleAttempt') return () => limitModel(target.singleAttempt?.() ?? target, quota)
    const value = Reflect.get(target, property, target) as unknown
    if (['run', 'structured', 'compact'].includes(String(property))) {
      return (request: { signal?: AbortSignal }) => quota.run(() => Reflect.apply(value as Function, target, [request]), request.signal)
    }
    return typeof value === 'function' ? value.bind(target) : value
  } })
}
