;(async () => {
  const Benchmark = require('benchmark-util')
  const IORedisPubSubEngine = require('../dist/cjs/index').default
  const RedisPubSub = require('graphql-redis-subscriptions').RedisPubSub
  const IORedis = require('ioredis')

  let bench = new Benchmark()
  const TRIGGER = 'TRIGGER'
  const pub = new IORedis({
    host: 'codeshare.local',
    lazyConnect: true,
  })
  const sub = new IORedis({
    host: 'codeshare.local',
    lazyConnect: true,
  })

  const oldTest = async () => {
    const pubSub = new RedisPubSub({
      publisher: pub,
      subscriber: sub,
    })
    const items = pubSub.asyncIterator(TRIGGER)
    setTimeout(() => {
      let i = 10
      while (i > 0) {
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.publish(TRIGGER, { i })
        i--
      }
      setTimeout(() => items.return(), 0)
    }, 0)
    for await (let i of items) {
      // console.log('OLD', i)
    }
  }
  const newTest = async () => {
    const pubSub = new IORedisPubSubEngine({
      pub,
      sub,
    })
    const items = pubSub.asyncIterator(TRIGGER)
    setTimeout(() => {
      let i = 10
      while (i > 0) {
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.asyncIterator(TRIGGER).next()
        pubSub.publish(TRIGGER, { i })
        i--
      }
      setTimeout(() => items.return(), 0)
    }, 0)
    for await (let i of items) {
      // console.log('NEW', i)
    }
  }

  async function runBenchMarks() {
    bench.add(`old`, oldTest).add(`new`, newTest).setRunsPerUnit(200)

    let results = await bench.run({
      onCycle: ({ name, totals, samples, warmup }) => {
        console.log(
          `${name} x ${Math.round(totals.avg)} ops/sec ± ${
            Math.round((totals.stdDev / totals.avg) * 10000) / 100
          }% (${totals.runs} runs sampled)`,
        )
      },
    })

    let fastest = results.sort((a, b) =>
      a.totals.avg > b.totals.avg ? -1 : a.totals.avg < b.totals.avg ? 1 : 0,
    )[0].name

    console.log(`Fastest is: ${fastest}`)
  }
  await pub.connect()
  await sub.connect()
  await runBenchMarks()
  await pub.disconnect()
  await sub.disconnect()
})()
