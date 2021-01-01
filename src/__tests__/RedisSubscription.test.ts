import { RedisSubscription } from './../RedisSubcription'
import createRedisMock from './test_utils/createRedisMock'
import jestConfig from '../../jest.config'

describe('RedisSubscription', () => {
  describe('start', () => {
    it('should subscribe to triggerName', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should start after stop', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      await subscription.stop()
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPED"`)
      await subscription.start()
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should cancel stop if started while stop delay', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      subscription.stop()
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPING"`)
      subscription.start()
      await subscription.start()
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      expect(opts.sub.unsubscribe).not.toHaveBeenCalled()
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should start after stopping', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      subscription.stop()
      // @ts-ignore
      expect(subscription.stopDelayPromise).toMatchInlineSnapshot(`
        Promise {
          "clear": [Function],
        }
      `)
      // @ts-ignore
      await subscription.stopDelayPromise
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPING"`)
      const startPromise1 = subscription.start()
      const startPromise2 = subscription.start()
      await startPromise1
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      await startPromise2
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should cancel start while force stopping', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      const stopPromise = subscription.stop({ force: true })
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPING"`)
      await expect(subscription.start()).rejects.toMatchInlineSnapshot(
        `[AbortError: start aborted, subscription is stopping (forced)]`,
      )
      await stopPromise
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPED"`)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should start after stop after start after stop', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      subscription.stop()
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPING"`)
      await subscription.start()
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      subscription.stop()
      expect(subscription.state).toMatchInlineSnapshot(`"STOPPING"`)
      await subscription.start()
      expect(subscription.state).toMatchInlineSnapshot(`"STARTED"`)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(0)
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(opts.triggerName)
    })
  })

  describe('stop', () => {
    it('should unsubscribe from triggerName', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
      }
      const subscription = new RedisSubscription(opts as any)
      await subscription.start()
      await subscription.stop()
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(opts.triggerName)
    })

    it('should not stop if starting', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
        logger: {
          warn: jest.fn(),
        },
      }
      const subscription = new RedisSubscription(opts as any)
      let startErr
      subscription.start().catch((err) => {
        startErr = err
      })
      expect(subscription.state).toMatchInlineSnapshot(`"STARTING"`)
      await expect(subscription.stop()).rejects.toMatchInlineSnapshot(
        `[AbortError: stop aborted, subscription is starting]`,
      )
      expect(opts.sub.unsubscribe).not.toHaveBeenCalled()
      expect(startErr).toMatchInlineSnapshot(`undefined`)
    })

    it('should force stop if starting', async () => {
      const opts = {
        triggerName: 'triggerName',
        sub: createRedisMock(),
        stopDelayDuration: 0,
        logger: {
          warn: jest.fn(),
        },
      }
      const subscription = new RedisSubscription(opts as any)
      let startErr
      subscription.start().catch((err) => {
        startErr = err
      })
      expect(subscription.state).toMatchInlineSnapshot(`"STARTING"`)
      await subscription.stop({ force: true })
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(opts.triggerName)
      expect(startErr).toMatchInlineSnapshot(
        `[Error: server started successfully, but is stopping now]`,
      )
    })
  })
})
