# graphql-ioredis-subscriptions

A graphql subscriptions PubSubEngine using IORedis that works with Apollo Server in TypeScript

# Installation

```sh
npm i --save graphql-ioredis-subscriptions
```

# Usage

#### Supports both ESM and CommonJS

```js
// esm
import RedisPubSubEngine from 'graphql-ioredis-subscriptions`
// commonjs
const RedisPubSubEngine = require('graphql-ioredis-subscriptions')
```

#### Use it with Type-GraphQL and ApolloServer

```js
import RedisPubSubEngine from 'graphql-ioredis-subscriptions'
import { ApolloServer } from 'apollo-server'
import { buildSchema } from 'type-graphql'
import IORedis from 'ioredis'

const schema = buildSchema({
  //...
  pubSub: new RedisPubSubEngine({
    /* required */
    pub: new IORedis(),
    sub: new IORedis(),

    /* optional */
    // defaults to JSON
    parser: {
      stringify: (val) => JSON.stringify(val)
      parse: (str) => JSON.parse(str)
    },
    // defaults to console
    logger: {
      warn: (...args) => console.warn(...args)
      error: (...args) => console.error(...args)
    }
  })
})

const server = new ApolloServer({
  schema,
  // other options...
})

// ...
```

# License

MIT
