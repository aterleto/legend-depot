//  Copyright 2021 Goldman Sachs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package org.finos.legend.depot.store.redis.core;

import io.opentracing.Tracer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

import javax.inject.Singleton;

@Singleton
public class RedisTracingConnectionFactory extends RedisConnectionFactory
{

    public RedisTracingConnectionFactory(String applicationName, RedisConfiguration configuration, Tracer tracer)
    {
        super(applicationName, configuration);
        this.database = initClient(tracer);
    }

    private UnifiedJedis initClient(Tracer tracer)
    {
        // TODO // see if https://github.com/opentracing-contrib/java-redis-client would work even though
        // TODO // it isn't up to date and doesn't have the redisearch fields wrapped
        return new JedisPooled(new HostAndPort(this.getConnectionURI(), this.getPort()));
       //return new TracingMongoClient(new TracingCommandListener.Builder(tracer).build(), mongoURI);
    }
}
