import {
  ListenerType,
  RedisPatternSubscription,
  RedisSubscription,
} from './RedisSubcription'

import BaseError from 'baseerr'
import { EventEmitter } from 'events'
import IORedis from 'ioredis'
import { PubSubEngine } from 'graphql-subscriptions'
import PullQueue from 'promise-pull-queue'
import abortable from 'abortable-generator'
import { state } from 'abstract-startable'

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
export type LoggerType = {
  warn(...data: any[]): any
  error(...data: any[]): any
  debug(...data: any[]): any
}

export interface OptsType<T> {
  parser?: {
    stringify: (val: T) => string
    parse: (val: string) => T
  }
  logger?: LoggerType
  unsubscribeDelayDuration?: number
  pub: IORedis.Redis
  sub: IORedis.Redis
}

type SubsByTriggerType = Record<string, RedisSubscription | undefined>
type SubsByPatternType = Record<string, RedisPatternSubscription | undefined>

export class RedisPubSubEngine<T> extends EventEmitter implements PubSubEngine {
  private opts: OptsType<T>
  private logger: NonNullable<OptsType<T>['logger']>
  private parser: NonNullable<OptsType<T>['parser']>
  // subscription state
  private subsByTrigger: SubsByTriggerType
  private subsByPattern: SubsByPatternType
  private removeListenerById: Record<
    number,
    {
      pattern: boolean
      triggerName: string
      removeListener: () => Promise<void>
    }
  >

  constructor(opts: OptsType<T>) {
    super()
    if (opts.pub === opts.sub) throw new Error('pub cannot equal sub')
    this.logger = opts?.logger ?? console
    this.parser = opts?.parser ?? JSON
    this.opts = opts

    // initial state
    this.subsByTrigger = {}
    this.subsByPattern = {}
    this.removeListenerById = {}

    // event handlers
    this.opts.sub.on('message', (triggerName: string, payloadStr: string) => {
      const listeners = this.subsByTrigger[triggerName]?.listeners
      if (listeners == null || listeners.size === 0) return
      let payload: T
      try {
        payload = this.parser.parse(payloadStr)
      } catch (err) {
        this.logger.error('message payload parse error', {
          err: PayloadParseError.wrap(
            err as Error,
            'message payload parse error',
            {
              payloadStr,
            },
          ),
        })
        return
      }
      listeners.forEach((onMessage) => onMessage(payload))
    })
    this.opts.sub.on('pmessage', (triggerName: string, payloadStr: string) => {
      const listeners = this.subsByPattern[triggerName]?.listeners
      if (listeners == null || listeners.size === 0) return
      let payload: T
      try {
        payload = this.parser.parse(payloadStr)
      } catch (err) {
        this.logger.error('message payload parse error', {
          err: PayloadParseError.wrap(
            err as Error,
            'message payload parse error',
            {
              payloadStr,
            },
          ),
        })
        return
      }
      listeners.forEach((onMessage) => onMessage(payload))
    })
  }

  nextId = (() => {
    let listenerId = 0
    return () => ++listenerId
  })()

  async publish(triggerName: string, payload: T): Promise<void> {
    await this.opts.pub.publish(triggerName, this.parser.stringify(payload))
  }

  async subscribe(
    triggerName: string,
    listener: ListenerType,
    options: Object = { pattern: false },
  ): Promise<number> {
    const pattern = (options as any)?.pattern ?? false

    let removeListener
    if (pattern) {
      const sub =
        this.subsByPattern[triggerName] ??
        new RedisPatternSubscription({
          triggerName,
          logger: this.logger,
          sub: this.opts.sub,
          stopDelayDuration: this.opts.unsubscribeDelayDuration,
        })
      this.subsByPattern[triggerName] = sub
      removeListener = await sub.addListener(listener)
    } else {
      const sub =
        this.subsByTrigger[triggerName] ??
        new RedisSubscription({
          triggerName,
          logger: this.logger,
          sub: this.opts.sub,
          stopDelayDuration: this.opts.unsubscribeDelayDuration,
        })
      this.subsByTrigger[triggerName] = sub
      removeListener = await sub.addListener(listener)
    }

    const id = this.nextId()
    this.removeListenerById[id] = {
      pattern,
      triggerName,
      removeListener,
    }

    return id
  }

  async unsubscribe(id: number): Promise<void> {
    const { removeListener, triggerName, pattern } =
      this.removeListenerById[id] ?? {}
    if (removeListener == null) {
      this.logger.warn('cannot unsubscribe from unknown subscription', { id })
      return
    }
    delete this.removeListenerById[id]
    try {
      await removeListener()
    } finally {
      const sub = pattern
        ? this.subsByPattern[triggerName]
        : this.subsByTrigger[triggerName]
      if (sub?.state == state.STOPPED) {
        if (pattern) {
          delete this.subsByPattern[triggerName]
        } else {
          delete this.subsByTrigger[triggerName]
        }
      }
    }
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
        if ((err as any).name === 'AbortError') return
        throw err
      } finally {
        subIds?.forEach((id) => self.unsubscribe(id))
      }
    })(signal)
  }

  __debugSubState(): {
    subsByTrigger: SubsByTriggerType
    subsByPattern: SubsByPatternType
  } {
    return {
      subsByTrigger: this.subsByTrigger,
      subsByPattern: this.subsByPattern,
    }
  }
}
