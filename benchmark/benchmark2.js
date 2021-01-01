const Benchmark = require('benchmark')
const RedisPubSubEngine = require('../dist/cjs/index').default
const RedisPubSub = require('graphql-redis-subscriptions').RedisPubSub
const IORedis = require('ioredis')

var suite = new Benchmark.Suite()
const TRIGGER = 'TRIGGER'
const pub = new IORedis({
  host: 'codeshare.local',
  lazyConnect: true,
})
const sub = new IORedis({
  host: 'codeshare.local',
  lazyConnect: true,
})

// add tests
// suite
//   .add('for..of', function () {
//     const arr = [1, 2, 3, 4, 5]
//     for (let i of arr) {
//       //
//     }
//   })
//   .add('forEach', function () {
//     const arr = [1, 2, 3, 4, 5]
//     arr.forEach(() => {
//       //
//     })
//   })
//   .add('for loop', function () {
//     const arr = [1, 2, 3, 4, 5]
//     for (let i = 0; i < arr.length; i++) {
//       //
//     }
//   })
//   // add listeners
//   .on('cycle', function (event) {
//     console.log(String(event.target))
//   })
//   .on('complete', function () {
//     console.log('Fastest is ' + this.filter('fastest').map('name'))
//   })
//   // run async
//   .run({ async: true })

// // add tests
suite
  .add('RedisPubSubEngine', function () {
    const i = new RedisPubSubEngine({
      pub,
      sub,
    }).asyncIterator(TRIGGER)
    i.next()
    // i.return()
  })
  .add('RedisPubSub', function () {
    const i = new RedisPubSub({
      publisher: pub,
      subscriber: sub,
    }).asyncIterator(TRIGGER)
    i.next()
    // i.return()
  })
  // add listeners
  .on('cycle', function (event) {
    console.log(String(event.target))
  })
  .on('complete', function () {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  // run async
  .run({ async: true })

// suite
//   .add('RedisPubSubEngine events', async () => {
//     const pubSub = new RedisPubSub({
//       publisher: pub,
//       subscriber: sub,
//     })
//     const items1 = pubSub.asyncIterator(TRIGGER)
//     // const items2 = pubSub.asyncIterator(TRIGGER)
//     setTimeout(() => {
//       let i = 10
//       while (i > 0) {
//         pubSub.publish(TRIGGER, { i })
//         i--
//       }
//       setTimeout(() => items1.return(), 0)
//       // setTimeout(() => items2.return(), 0)
//     }, 0)
//     // for await (let i of items1) {
//     //   // console.log('NEW', i)
//     // }
//     // for await (let i of items2) {
//     //   // console.log('NEW', i)
//     // }
//   })
//   .add('RedisPubSub events', async () => {
//     const pubSub = new RedisPubSubEngine({
//       pub,
//       sub,
//     })
//     const items1 = pubSub.asyncIterator(TRIGGER)
//     const items2 = pubSub.asyncIterator(TRIGGER)
//     setTimeout(() => {
//       let i = 10
//       while (i > 0) {
//         pubSub.publish(TRIGGER, { i })
//         i--
//       }
//       setTimeout(() => items1.return(), 0)
//       setTimeout(() => items2.return(), 0)
//     }, 0)
//     for await (let i of items1) {
//       // console.log('NEW', i)
//     }
//     for await (let i of items2) {
//       // console.log('NEW', i)
//     }
//   })
//   // add listeners
//   .on('cycle', function (event) {
//     console.log(String(event.target))
//   })
//   .on('complete', function () {
//     console.log('Fastest is ' + this.filter('fastest').map('name'))
//   })
//   // run async
//   .run({ async: true })
