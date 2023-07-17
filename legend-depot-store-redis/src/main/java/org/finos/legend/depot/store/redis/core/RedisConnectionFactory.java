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

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

public abstract class RedisConnectionFactory implements ConnectionFactory
{
    private final String host;
    private final String applicationName;
    private final int port;
    protected UnifiedJedis database;

    public RedisConnectionFactory(String applicationName, RedisConfiguration redisConfiguration)
    {
        if (redisConfiguration == null || isNullOrEmpty(redisConfiguration.url) || isNullOrEmpty(redisConfiguration.port))
        {
            throw new IllegalArgumentException("Invalid redis configuration provided");
        }

        //TODO validate port is an integer

        this.applicationName = applicationName;
        this.host = redisConfiguration.url;
        this.port = Integer.parseInt(redisConfiguration.port);
    }

    private boolean isNullOrEmpty(String string)
    {
        return string == null || string.isEmpty();
    }

    public String getApplicationName()
    {
        return applicationName;
    }

    public String getConnectionURI()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    @Override
    public UnifiedJedis getDatabase()
    {
        return new JedisPooled(new HostAndPort(host, port), DefaultJedisClientConfig.builder().build());
    }

}
