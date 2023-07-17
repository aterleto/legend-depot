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

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import javax.inject.Singleton;

@Singleton
public class RedisNonTracingConnectionFactory extends RedisConnectionFactory
{
    public RedisNonTracingConnectionFactory(String applicationName, RedisConfiguration configuration)
    {
        super(applicationName, configuration);
        database = new JedisPooled(new HostAndPort(configuration.url, Integer.parseInt(configuration.port)));
    }
}
