const Benchmark = require('benchmark-util')
const RedisPubSubEngine = require('../dist/cjs/index').default
const RedisPubSub = require('graphql-redis-subscriptions').RedisPubSub
const IORedis = require('ioredis')

async function runBenchMarks(getBench) {
  const bench = getBench()
  const results = await bench.run({
    onCycle: ({ name, totals, samples, warmup }) => {
      console.log(
        `${name} x ${Math.round(totals.avg)} ops/sec ± ${
          Math.round((totals.stdDev / totals.avg) * 10000) / 100
        }% (${totals.runs} runs sampled)`,
      )
    },
  })

  const fastest = results.sort((a, b) =>
    a.totals.avg > b.totals.avg ? -1 : a.totals.avg < b.totals.avg ? 1 : 0,
  )[0].name

  console.log(`Fastest is: ${fastest}`)
}

;(async () => {
  const pub = new IORedis({
    host: 'codeshare.local',
    lazyConnect: true,
  })
  const sub = new IORedis({
    host: 'codeshare.local',
    lazyConnect: true,
  })
  await pub.connect()
  await sub.connect()
  // await runBenchMarks(() => {
  //   const TRIGGER = 'TRIGGER'

    return new Benchmark()
      .add(`old: getting results from 2 async iterators`, async () => {
        const pubSub = new RedisPubSub({
          publisher: pub,
          subscriber: sub,
        })
        const items1 = pubSub.asyncIterator(TRIGGER)
        const items2 = pubSub.asyncIterator(TRIGGER)
        setTimeout(() => {
          let i = 10
          while (i > 0) {
            pubSub.publish(TRIGGER, { i })
            i--
          }
          setTimeout(() => items1.return(), 0)
          setTimeout(() => items2.return(), 0)
        }, 0)
        for await (let i of items1) {
          // console.log('NEW', i)
        }
        for await (let i of items2) {
          // console.log('NEW', i)
        }
      })
      .add(`new: getting results from 2 async iterators`, async () => {
        const pubSub = new RedisPubSubEngine({
          pub,
          sub,
        })
        const items1 = pubSub.asyncIterator(TRIGGER)
        const items2 = pubSub.asyncIterator(TRIGGER)
        setTimeout(() => {
          let i = 10
          while (i > 0) {
            pubSub.publish(TRIGGER, { i })
            i--
          }
          setTimeout(() => items1.return(), 0)
          setTimeout(() => items2.return(), 0)
        }, 0)
        for await (let i of items1) {
          // console.log('NEW', i)
        }
        for await (let i of items2) {
          // console.log('NEW', i)
        }
      })
      .setRunsPerUnit(10)
  })
  await runBenchMarks(() => {
    const TRIGGER = 'TRIGGER'
    const pub = new IORedis({
      host: 'codeshare.local',
      lazyConnect: true,
    })
    const sub = new IORedis({
      host: 'codeshare.local',
      lazyConnect: true,
    })
    return new Benchmark()
      .add(`new: create and subscribe async iterators`, () => {
        new RedisPubSubEngine({
          publisher: pub,
          subscriber: sub,
          pub,
          sub,
        })
          .asyncIterator(TRIGGER)
          .next()
        new RedisPubSub({
          publisher: pub,
          subscriber: sub,
          pub,
          sub,
        })
          .asyncIterator(TRIGGER)
          .next()
      })
      .add(`old: create and subscribe async iterators`, () => {
        new RedisPubSub({
          publisher: pub,
          subscriber: sub,
        })
          .asyncIterator(TRIGGER)
          .next()
        new RedisPubSub({
          publisher: pub,
          subscriber: sub,
        })
          .asyncIterator(TRIGGER)
          .next()
      })
      .setRunsPerUnit(25)
  })
  await pub.disconnect()
  await sub.disconnect()
})()
