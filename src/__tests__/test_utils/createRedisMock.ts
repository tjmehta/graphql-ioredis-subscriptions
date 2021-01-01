import { EventEmitter } from 'events'

export default function createRedisMock(realEE?: boolean) {
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
