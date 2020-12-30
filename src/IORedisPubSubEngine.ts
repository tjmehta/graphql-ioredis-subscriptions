import pDefer, { DeferredPromise } from 'p-defer'

import BaseError from 'baseerr'
import { EventEmitter } from 'events'
import IORedis from 'ioredis'
import PQueue from 'p-queue'
import { PubSubEngine } from 'graphql-subscriptions'
import PullQueue from 'promise-pull-queue'
import abortable from 'abortable-generator'
import assert from 'assert'

const noop = () => {}

export class AbortError extends BaseError<{}> {}
export class PayloadParseError extends BaseError<{ payloadStr: string }> {}
export class ReceivedPayloadAfterUnsubscribeError extends BaseError<{
  triggerName: string
}> {}
export class UnsubscribeError extends BaseError<{
  id: number
  triggerName: string
  pattern: boolean
}> {}

export interface IORedisPubSubOptions<T> {
  parser?: {
    stringify: (val: T) => string
    parse: (val: string) => T
  }
  logger?: {
    warn(...data: any[]): any
    error(...data: any[]): any
  }
  pub: IORedis.Redis
  sub: IORedis.Redis
}

export interface IORedisPubSubSubscriptionOptions {
  pattern: true
}

export type SubscriptionInfo<T> = {
  triggerName: string
  pattern: boolean
  onMessage: (payload: T) => void
}

export class IORedisPubSubEngine<T>
  extends EventEmitter
  implements PubSubEngine {
  private logger: NonNullable<IORedisPubSubOptions<T>['logger']>
  //redis state
  private parser: NonNullable<IORedisPubSubOptions<T>['parser']>
  private pub: IORedis.Redis
  private sub: IORedis.Redis
  // subscription state
  private counter: number
  private subInfoById: Record<number, SubscriptionInfo<T>>
  private activeSubsByTrigger: Record<string, Set<number>>
  private subQueues: Record<string, PQueue>

  constructor(options: IORedisPubSubOptions<T>) {
    super()
    // initial state
    this.logger = options?.logger ?? console
    this.parser = options?.parser ?? JSON
    if (options.pub === options.sub) throw new Error('pub cannot equal sub')
    this.pub = options.pub
    this.sub = options.sub
    this.counter = 0
    this.subInfoById = {}
    this.activeSubsByTrigger = {}
    this.subQueues = {}
    // event handlers
    this.sub.on('message', (triggerName: string, payloadStr: string) => {
      const activeSubs = this.activeSubsByTrigger[triggerName]
      if (!Boolean(activeSubs?.size)) return
      let payload: T
      try {
        payload = this.parser.parse(payloadStr)
      } catch (err) {
        this.logger.error('message payload parse error', {
          err: PayloadParseError.wrap(err, 'message payload parse error', {
            payloadStr,
          }),
        })
        return
      }
      activeSubs?.forEach((id) => {
        const { onMessage, pattern } = this.subInfoById[id] || {}
        if (onMessage == null || onMessage === noop) return
        if (pattern) return
        onMessage(payload)
      })
    })
    this.sub.on('pmessage', (triggerName: string, payloadStr: string) => {
      const activeSubs = this.activeSubsByTrigger[triggerName]
      if (!Boolean(activeSubs?.size)) return
      let payload: T
      try {
        payload = this.parser.parse(payloadStr)
      } catch (err) {
        this.logger.error('message payload parse error', {
          err: PayloadParseError.wrap(err, 'message payload parse error', {
            payloadStr,
          }),
        })
        return
      }
      this.activeSubsByTrigger[triggerName]?.forEach((id) => {
        const { onMessage, pattern } = this.subInfoById[id] || {}
        // sanity check, shouldn't happen
        if (onMessage == null || onMessage === noop) return
        if (pattern === false) return
        onMessage(payload)
      })
    })
  }

  private async queue<TaskResult>(
    triggerName: string,
    task: () => Promise<TaskResult>,
  ): Promise<TaskResult> {
    let queue = this.subQueues[triggerName]

    if (queue == null) {
      queue = this.subQueues[triggerName] = new PQueue({
        concurrency: 1,
      })
      queue.onEmpty().then(() => {
        // note: not sure if this check is necessary
        if (this.subQueues[triggerName] === queue)
          delete this.subQueues[triggerName]
      })
    }

    return queue.add(task)
  }

  async publish(triggerName: string, payload: T): Promise<void> {
    await this.pub.publish(triggerName, this.parser.stringify(payload))
  }

  async subscribe(
    triggerName: string,
    onMessage: SubscriptionInfo<T>['onMessage'],
    options: Object = { pattern: false },
  ): Promise<number> {
    this.counter++
    const id = this.counter
    const pattern = (options as any)?.pattern ?? false
    this.subInfoById[id] = {
      triggerName,
      pattern,
      onMessage,
    }
    return this.queue(triggerName, async () => {
      const activeSubs = this.activeSubsByTrigger[triggerName]

      if (activeSubs == null) {
        // new subscription
        if (pattern) {
          await this.sub.psubscribe(triggerName)
        } else {
          await this.sub.subscribe(triggerName)
        }
        // successful, add to actives
        this.activeSubsByTrigger[triggerName] = new Set([id])
      } else {
        activeSubs.add(id)
      }

      return id
    })
  }

  async unsubscribe(id: number, debug?: string): Promise<void> {
    const subInfo = this.subInfoById[id] ?? {}
    const { triggerName, onMessage, pattern } = subInfo

    if (onMessage === noop) {
      this.logger.warn(
        'cannot unsubscribe from already unsubscribed subscription',
        { id, triggerName, pattern },
      )
      return
    }
    // check if already unsubscribed
    if (triggerName == null) {
      this.logger.warn('cannot unsubscribe from unknown subscription', { id })
      return
    }

    // clear listener immediately
    subInfo.onMessage = noop
    return this.queue(triggerName, async () => {
      const { triggerName, pattern } = this.subInfoById[id] ?? {}
      // check if already unsubscribed
      if (triggerName == null) {
        this.logger.warn('cannot unsubscribe from unknown subscription', { id })
        return
      }

      const activeSubs = this.activeSubsByTrigger[triggerName]
      // sanity check if subscription exists (should never happen)
      assert(
        activeSubs,
        // don't create an error if the assertion passes
        activeSubs
          ? 'never'
          : new UnsubscribeError(
              'unexpected state no activeSubs in unsubscribe',
              { id, triggerName, pattern },
            ),
      )
      // clean up active state
      delete this.subInfoById[id]
      activeSubs.delete(id)
      if (activeSubs.size === 0) {
        // unsubscribe
        if (pattern) {
          await this.sub.punsubscribe(triggerName)
        } else {
          await this.sub.unsubscribe(triggerName)
        }
        delete this.activeSubsByTrigger[triggerName]
      }
    })
  }

  asyncIterator<TT = T>(
    triggers: string | string[],
    signal?: AbortSignal,
  ): AsyncIterableIterator<TT> & { done: boolean } {
    const self = this
    return abortable<TT>(async function* (raceAbort) {
      const subIds = []
      try {
        const triggerNames = Array.isArray(triggers) ? triggers : [triggers]
        const pullQueue = new PullQueue<TT>()
        // subscribe to triggerName(s)
        for (let triggerName of triggerNames) {
          let subId = await raceAbort(async (signal) => {
            const subId = await self.subscribe(triggerName, (payload) => {
              pullQueue.pushValue((payload as any) as TT)
            })
            if (signal.aborted) self.unsubscribe(subId).catch(() => {})
            return subId
          })
          subIds.push(subId)
        }
        // yield payloads
        while (true) {
          yield raceAbort((signal) => pullQueue.pull(signal))
        }
      } catch (err) {
        if (err.name === 'AbortError') return
        throw err
      } finally {
        subIds?.forEach((id) => self.unsubscribe(id))
      }
    })(signal)
  }
}
