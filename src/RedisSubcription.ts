import { AbortError, LoggerType } from './RedisPubSubEngine'
import AbstractStartable, {
  StartOptsType,
  StopOptsType,
  state,
} from 'abstract-startable'
import DoublyLinkedList, { DoublyNode } from 'doubly'

import AbortController from 'fast-abort-controller'
import IORedis from 'ioredis'
import { ignoreName } from 'ignore-errors'
import timeout from 'abortable-timeout'

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
  protected stopDelayController: AbortController | null

  forceStopping: boolean
  listeners: DoublyLinkedList<ListenerType>

  constructor(opts: OptsType) {
    super()
    this.opts = opts
    this.stopDelayPromise = null
    this.stopDelayController = null
    this.forceStopping = false
    this.logger = this.opts.logger ?? console
    this.listeners = new DoublyLinkedList()
  }

  async addListener(
    cb: <T>(payload: T) => unknown,
  ): Promise<() => Promise<void>> {
    let removed = false
    this.listeners.push(cb)
    const node = this.listeners.tail as DoublyNode<ListenerType>
    try {
      // ensure subscription is "started"
      await this.start()
    } catch (err) {
      // subscription failed remove listener
      removed = true
      this.listeners.deleteNode(node)
      throw err
    }

    const removeListener = async () => {
      if (removed) return
      removed = true
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
      if (this.stopDelayController) {
        this.stopDelayController.abort()
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

    if (this.opts.stopDelayDuration == null) {
      return this.__stop()
    }

    const controller = (this.stopDelayController = new AbortController())
    this.stopDelayPromise = timeout(
      this.opts.stopDelayDuration,
      controller.signal,
    )
    try {
      await this.stopDelayPromise
      this.stopDelayController = null
      this.stopDelayPromise = null
      return this.__stop()
    } catch (err) {
      // aborted
      this.stopDelayController = null
      this.stopDelayPromise = null
    }
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
