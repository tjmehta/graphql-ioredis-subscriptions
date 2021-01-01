import { AbortError, LoggerType } from './RedisPubSubEngine'
import AbstractStartable, {
  StartOptsType,
  StopOptsType,
  state,
} from 'abstract-startable'

import { DoublyLinkedList } from './DoublyLinkedList'
import IORedis from 'ioredis'
import { ignoreName } from 'ignore-errors'
import timeout from 'timeout-then'

type OptsType = {
  logger?: LoggerType
  triggerName: string
  sub: IORedis.Redis
  stopDelayDuration?: number
}

export type ListenerType = <T>(payload: T) => unknown

export class RedisSubscription extends AbstractStartable {
  protected opts: OptsType
  protected logger: LoggerType
  protected stopDelayPromise: ReturnType<typeof timeout> | null

  forceStopping: boolean
  listeners: DoublyLinkedList<ListenerType>

  constructor(opts: OptsType) {
    super()
    this.opts = opts
    this.stopDelayPromise = null
    this.forceStopping = false
    this.logger = this.opts.logger ?? console
    this.listeners = new DoublyLinkedList()
  }

  async addListener(
    cb: <T>(payload: T) => unknown,
  ): Promise<() => Promise<void>> {
    const node = this.listeners.push(cb)
    // ensure subscription is "started"
    await this.start()
    const removeListener = async () => {
      this.listeners.deleteNode(node)
      if (this.listeners.size === 0) {
        this.stop().catch(ignoreName('AbortError'))
      }
      await this.stopPromise?.catch(ignoreName('AbortError'))
    }

    return removeListener
  }

  async start(opts?: StartOptsType) {
    if (this.state === state.STOPPING) {
      if (this.forceStopping) {
        throw new AbortError('start aborted, subscription is stopping (forced)')
      }
      if (this.stopDelayPromise) {
        this.stopDelayPromise.clear()
        delete this.stopPromise
        return
      }
      // not forceStopping
      if (this.startPromise) return this.startPromise
      this.startPromise = this.stopPromise?.then(() => {
        delete this.startPromise
        return super.start(opts)
      })
      return this.startPromise
    }
    return super.start(opts)
  }

  async stop(opts?: StopOptsType) {
    if (this.state === state.STARTING) {
      // only stop it when forced
      if (opts?.force) return super.stop(opts)
      // let it start..
      throw new AbortError('stop aborted, subscription is starting')
    }
    return super.stop(opts)
  }

  protected async _start() {
    await this.opts.sub.subscribe(this.opts.triggerName)
  }

  protected async _stop(opts?: StopOptsType): Promise<void> {
    if (opts?.force) {
      this.forceStopping = true
      return this.__stop().finally(() => {
        this.forceStopping = false
      })
    }

    if (this.opts.stopDelayDuration != null) {
      this.stopDelayPromise = timeout(this.opts.stopDelayDuration)
      await this.stopDelayPromise.finally(() => {
        this.stopDelayPromise = null
      })
    }

    return this.__stop()
  }

  protected async __stop() {
    await this.opts.sub.unsubscribe(this.opts.triggerName)
  }
}

export class RedisPatternSubscription extends RedisSubscription {
  protected async _start() {
    await this.opts.sub.psubscribe(this.opts.triggerName)
  }

  protected async __stop() {
    await this.opts.sub.punsubscribe(this.opts.triggerName)
  }
}
