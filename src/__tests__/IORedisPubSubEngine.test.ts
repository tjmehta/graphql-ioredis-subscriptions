import { EventEmitter } from 'events'
import IORedisPubSubEngine from './../index'
import timeout from 'timeout-then'

const createRedisMock = (realEE?: boolean) => {
  class RedisMock extends EventEmitter {
    publish = jest.fn()
    subscribe = jest.fn()
    psubscribe = jest.fn()
    unsubscribe = jest.fn()
    punsubscribe = jest.fn()
  }
  const mock = new RedisMock()
  jest.spyOn(mock, 'on')
  return mock
}

type PayloadType = { foo: number }

describe('IORedisPubSubEngine', () => {
  it('should create an instance', () => {
    const opts = {
      pub: createRedisMock(),
      sub: createRedisMock(),
    }
    const pubsub = new IORedisPubSubEngine(opts as any)
    expect(pubsub).toBeInstanceOf(IORedisPubSubEngine)
    expect(opts.pub.on).not.toHaveBeenCalled()
    expect(opts.sub.on).toHaveBeenCalledWith('message', expect.any(Function))
    expect(opts.sub.on).toHaveBeenCalledWith('pmessage', expect.any(Function))
  })

  describe('publish', () => {
    it('should reject with publish error', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const err = new Error('boom')
      opts.pub.publish.mockRejectedValue(err)
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      expect(pubsub.publish('triggerName', { foo: 10 })).rejects.toThrow(err)
    })

    it('should stringify and publish payload to redis (default parser)', async () => {
      jest.spyOn(JSON, 'stringify')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const payload = { foo: 10 }
      await pubsub.publish(triggerName, payload)
      expect(JSON.stringify).toHaveBeenCalledWith(payload)
      expect(opts.pub.publish.mock.calls).toMatchInlineSnapshot(`
        Array [
          Array [
            "triggerName",
            "{\\"foo\\":10}",
          ],
        ]
      `)
    })

    it('should stringify and publish payload to redis (custom parser)', async () => {
      jest.spyOn(JSON, 'stringify')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        parser: {
          stringify: jest.fn().mockReturnValue('STRING'),
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const payload = { foo: 10 }
      await pubsub.publish(triggerName, payload)
      expect(JSON.stringify).not.toHaveBeenCalled()
      expect(opts.pub.publish.mock.calls).toMatchInlineSnapshot(`
        Array [
          Array [
            "triggerName",
            "STRING",
          ],
        ]
      `)
    })
  })

  describe('subscribe', () => {
    it('should reject with subscribe error', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      const err = new Error('boom')
      opts.sub.subscribe.mockRejectedValue(err)
      await expect(pubsub.subscribe(triggerName, onMessage)).rejects.toThrow(
        err,
      )
    })

    it('should subscribe by event name', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(triggerName)
    })

    it('should subscribe by pattern', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage, { pattern: true })
      expect(opts.sub.psubscribe).toHaveBeenCalledWith(triggerName)
    })

    it('should subscribe to the same event one-by-one in-order', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      const order = []
      await Promise.all([
        (async () => {
          // this would finish second if there wasn't a queue
          opts.sub.subscribe.mockReturnValue(timeout(100))
          await pubsub.subscribe(triggerName, onMessage)
          order.push('index 0')
        })(),
        (async () => {
          // this would finish first if there wasn't a queue
          opts.sub.subscribe.mockReturnValue(timeout(50))
          await pubsub.subscribe(triggerName, onMessage)
          order.push('index 1')
        })(),
      ])
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(triggerName)
      expect(order).toMatchInlineSnapshot(`
        Array [
          "index 0",
          "index 1",
        ]
      `)
    })
  })

  describe('unsubscribe', () => {
    it('should warn if id is unknown', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: {
          warn: jest.fn(),
          error: jest.fn(),
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      await expect(pubsub.unsubscribe(999)).resolves.toBeUndefined()
      expect(opts.logger.warn.mock.calls).toMatchInlineSnapshot(`
        Array [
          Array [
            "cannot unsubscribe from unknown subscription",
            Object {
              "id": 999,
            },
          ],
        ]
      `)
    })

    it('should unsubscribe from subscribed event name', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)

      const triggerName = 'evt'
      const subId = await pubsub.subscribe(triggerName, () => {})
      await expect(pubsub.unsubscribe(subId)).resolves.toBeUndefined()
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName)
      expect(opts.sub.punsubscribe).not.toHaveBeenCalled()
    })

    it('should unsubscribe from subscribed pattern', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)

      const triggerName = 'pattern'
      const subId = await pubsub.subscribe(triggerName, () => {}, {
        pattern: true,
      })
      await expect(pubsub.unsubscribe(subId)).resolves.toBeUndefined()
      expect(opts.sub.punsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.punsubscribe).toHaveBeenCalledWith(triggerName)
      expect(opts.sub.unsubscribe).not.toHaveBeenCalled()
    })

    it('should unsubscribe from the same event names one-by-one in-order', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: {
          warn: jest.fn(),
          error: jest.fn(),
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      // subscribe
      const triggerName = 'triggerName'
      const subId = await pubsub.subscribe(triggerName, () => {})
      // unsubscribe multiple times
      const order = []
      await Promise.all([
        (async () => {
          // this would finish second if there wasn't a queue
          opts.sub.unsubscribe.mockReturnValue(timeout(100))
          await pubsub.unsubscribe(subId)
          order.push('index 0')
        })(),
        (async () => {
          // this would finish first if there wasn't a queue
          opts.sub.unsubscribe.mockReturnValue(timeout(50))
          await pubsub.unsubscribe(subId)
          order.push('index 1')
        })(),
      ])
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(1)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName)
      expect(order).toMatchInlineSnapshot(`
        Array [
          "index 1",
          "index 0",
        ]
      `)
    })
  })

  describe('event handling', () => {
    it('should subscribe and handle event messages', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage)
      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(1)
      expect(onMessage).toHaveBeenCalledWith(payload)
    })

    it('should log event message parsing errors', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: {
          error: jest.fn(),
        },
        parser: () => {
          throw new Error('parse error')
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage)
      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(0)
      expect(opts.logger.error.mock.calls).toMatchInlineSnapshot(`
        Array [
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
        ]
      `)
    })

    it('should subscribe and handle pattern messages', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage, { pattern: true })
      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(1)
      expect(onMessage).toHaveBeenCalledWith(payload)
    })

    it('should log pattern message parsing errors', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: {
          error: jest.fn(),
        },
        parser: () => {
          throw new Error('parse error')
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      await pubsub.subscribe(triggerName, onMessage, { pattern: true })
      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(0)
      expect(opts.logger.error.mock.calls).toMatchInlineSnapshot(`
        Array [
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
          Array [
            "message payload parse error",
            Object {
              "err": [PayloadParseError: message payload parse error],
            },
          ],
        ]
      `)
    })

    it('should unsubscribe syncronously and and not handle event messages', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      const subId = await pubsub.subscribe(triggerName, onMessage)
      pubsub.unsubscribe(subId)

      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(0)
    })

    it('should unsubscribe syncronously and and not handle pattern messages', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      const subId = await pubsub.subscribe(triggerName, onMessage, {
        pattern: true,
      })
      pubsub.unsubscribe(subId)

      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('pmessage', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(0)
    })

    it('should unsubscribe (async) and and not handle messages', async () => {
      // note: this test is for coverage
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage = jest.fn()
      const subId = await pubsub.subscribe(triggerName, onMessage)
      await pubsub.unsubscribe(subId)

      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      expect(onMessage).toHaveBeenCalledTimes(0)
    })

    it('should only unsubscribe specific subscription', async () => {
      jest.spyOn(JSON, 'parse')
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const onMessage1 = jest.fn()
      const onMessage2 = jest.fn()
      const subId1 = await pubsub.subscribe(triggerName, onMessage1)
      const subId2 = await pubsub.subscribe(triggerName, onMessage2)
      pubsub.unsubscribe(subId1)

      // mock redis events
      const payload = { foo: 'bar' }
      opts.sub.emit('message', triggerName, JSON.stringify(payload))
      expect(onMessage1).toHaveBeenCalledTimes(0)
      expect(onMessage2).toHaveBeenCalledTimes(1)
      expect(onMessage2).toHaveBeenCalledWith(payload)
    })
  })

  describe('subscribe/unsubscribe scenarios and final state', () => {
    it('should subscribe and unsubscribe in expected order', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: {
          warn: jest.fn(),
          error: jest.fn(),
        },
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      // subscribe
      const triggerName1 = 'triggerName1'
      const triggerName2 = 'triggerName2'
      const sub1Id1 = await pubsub.subscribe(triggerName1, () => {})
      const sub1Id2 = await pubsub.subscribe(triggerName1, () => {})
      const sub2Id1 = await pubsub.subscribe(triggerName2, () => {})
      // unsubscribe multiple times
      const order = []
      await Promise.all([
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(25))
          await pubsub.unsubscribe(sub1Id1)
          order.push('index 1')
        })(),
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(25))
          await pubsub.unsubscribe(sub1Id2)
          // get's queued behind above
          order.push('index 2')
        })(),
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(60))
          await pubsub.unsubscribe(sub2Id1)
          // own queue, but longest timeout
          order.push('index 3')
        })(),
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(50)) // not used
          await pubsub.unsubscribe(sub2Id1)
          // finishes immediately bc already unsubscribed
          order.push('index 0')
        })(),
      ])
      // expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName1)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName2)
      expect(order).toMatchInlineSnapshot(`
        Array [
          "index 0",
          "index 1",
          "index 2",
          "index 3",
        ]
      `)
    })

    it('should subscribe and unsubscribe in expected order', async () => {
      // note: if this breaks in teh future it's not that important bc it's unsubscribing based on predicted
      // subscription ids which won't happen in the real world
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      // subscribe
      const triggerName = 'triggerName'
      // unsubscribe multiple times
      let subId1 = 1
      let subId2 = 2
      const order = []
      await Promise.all([
        (async () => {
          opts.sub.subscribe.mockReturnValue(timeout(20))
          await pubsub.subscribe(triggerName, () => {})
          order.push('index 0')
        })(),
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(20))
          await pubsub.unsubscribe(subId1)
          order.push('index 1')
        })(),
        (async () => {
          opts.sub.subscribe.mockReturnValue(timeout(20))
          await pubsub.subscribe(triggerName, () => {})
          order.push('index 2')
        })(),
        (async () => {
          opts.sub.unsubscribe.mockReturnValue(timeout(20))
          await pubsub.unsubscribe(subId2)
          order.push('index 3')
        })(),
      ])
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(triggerName)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName)
      expect(order).toMatchInlineSnapshot(`
        Array [
          "index 0",
          "index 1",
          "index 2",
          "index 3",
        ]
      `)
    })

    it('should subscribe and unsubscribe in expected order', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      // subscribe
      const triggerName = 'triggerName'
      // unsubscribe multiple times
      const order = []
      await Promise.all([
        (async () => {
          opts.sub.subscribe.mockReturnValue(timeout(20))
          const subId1 = await pubsub.subscribe(triggerName, () => {})
          order.push('index 0')
          opts.sub.unsubscribe.mockReturnValue(timeout(20))
          await Promise.all([
            (async () => {
              await pubsub.unsubscribe(subId1)
              order.push('index 1')
              opts.sub.subscribe.mockReturnValue(timeout(30))
            })(),
            (async () => {
              const subId2 = await pubsub.subscribe(triggerName, () => {})
              order.push('index 2')
              opts.sub.unsubscribe.mockReturnValue(timeout(20))
              await pubsub.unsubscribe(subId2)
              order.push('index 3')
            })(),
          ])
        })(),
      ])
      expect(opts.sub.subscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.subscribe).toHaveBeenCalledWith(triggerName)
      expect(opts.sub.unsubscribe).toHaveBeenCalledTimes(2)
      expect(opts.sub.unsubscribe).toHaveBeenCalledWith(triggerName)
      expect(order).toMatchInlineSnapshot(`
        Array [
          "index 0",
          "index 1",
          "index 2",
          "index 3",
        ]
      `)
    })
  })

  describe('asyncIterator', () => {
    it('should unsubscribe even if returned immediately', async () => {
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const iterable = pubsub.asyncIterator<PayloadType>(triggerName)
      const p = iterable.next()
      await iterable.return()
      await p
      expect(iterable.done).toBe(true)
    })

    it('should not get stuck on yielding same value forever', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const iterable = pubsub.asyncIterator<PayloadType>(triggerName)
      setTimeout(() => {
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'one' }))
        iterable.next()
        iterable.next()
        iterable.next()
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'one' }))
        iterable.next()
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'one' }))
        iterable.return()
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'one' }))
      }, 100)
      for await (let payload of iterable) {
        console.log(payload)
      }
    })

    it('should yield payloads for a triggerName', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName = 'triggerName'
      const iterable = pubsub.asyncIterator<PayloadType>(triggerName)
      setTimeout(() => {
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'one' }))
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'two' }))
        opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'thr' }))
        setTimeout(() => {
          opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'for' }))
          iterable.return()
          opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'fiv' }))
        }, 10)
      }, 10)
      const payloads = []
      try {
        for await (let payload of iterable) {
          payloads.push(payload)
        }
      } finally {
        payloads.push('finally')
      }
      opts.sub.emit('message', triggerName, JSON.stringify({ foo: 'six' }))
      expect(payloads).toMatchInlineSnapshot(`
        Array [
          Object {
            "foo": "one",
          },
          Object {
            "foo": "two",
          },
          Object {
            "foo": "thr",
          },
          "finally",
        ]
      `)
    })

    it('should yield payloads for multiple triggerNames', async () => {
      // create pub sub
      const opts = {
        pub: createRedisMock(),
        sub: createRedisMock(),
        logger: console,
      }
      const pubsub = new IORedisPubSubEngine<PayloadType>(opts as any)
      const triggerName1 = 'triggerName1'
      const triggerName2 = 'triggerName2'
      const iterable = pubsub.asyncIterator<PayloadType>([
        triggerName1,
        triggerName2,
      ])
      setTimeout(() => {
        opts.sub.emit('message', triggerName1, JSON.stringify({ foo: 'one' }))
        opts.sub.emit('message', triggerName2, JSON.stringify({ foo: 'two' }))
        opts.sub.emit('message', triggerName1, JSON.stringify({ foo: 'thr' }))
        setTimeout(() => {
          opts.sub.emit('message', triggerName2, JSON.stringify({ foo: 'for' }))
          iterable.return()
          opts.sub.emit('message', triggerName1, JSON.stringify({ foo: 'fiv' }))
        }, 10)
      }, 10)
      const payloads = []
      try {
        for await (let payload of iterable) {
          payloads.push(payload)
        }
      } finally {
        payloads.push('finally')
      }
      opts.sub.emit('message', triggerName1, JSON.stringify({ foo: 'six' }))
      opts.sub.emit('message', triggerName2, JSON.stringify({ foo: 'svn' }))
      expect(payloads).toMatchInlineSnapshot(`
        Array [
          Object {
            "foo": "one",
          },
          Object {
            "foo": "two",
          },
          Object {
            "foo": "thr",
          },
          "finally",
        ]
      `)
    })
  })
})
