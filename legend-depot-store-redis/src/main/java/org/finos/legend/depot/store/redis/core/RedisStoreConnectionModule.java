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

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.finos.legend.depot.store.api.StorageConfiguration;
import org.finos.legend.depot.tracing.configuration.OpenTracingConfiguration;
import org.finos.legend.depot.tracing.services.TracerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

import javax.inject.Named;

public class RedisStoreConnectionModule extends PrivateModule
{
    @Override
    protected void configure()
    {
        expose(ConnectionFactory.class);
        expose(UnifiedJedis.class).annotatedWith(Names.named("redisClient"));
        expose(Boolean.class).annotatedWith(Names.named("transactionMode"));
        //expose(MongoClient.class);
    }

    @Provides
    @Singleton
    RedisConfiguration getRedisConfiguration(StorageConfiguration configuration)
    {
        if (configuration != null && configuration instanceof RedisConfiguration)
        {
            return (RedisConfiguration) configuration;
        }
        else
        {
            throw new IllegalArgumentException("redis configuration not provided");
        }
    }

    @Provides
    @Singleton
    ConnectionFactory getConnectionFactory(@Named("applicationName") String applicationName, RedisConfiguration redisConfiguration, OpenTracingConfiguration openTracingConfiguration, TracerFactory tracerFactory)
    {
        if (openTracingConfiguration.isEnabled() && redisConfiguration.isTracingEnabled())
        {
            return new RedisTracingConnectionFactory(applicationName, redisConfiguration, tracerFactory.getTracer());
        }
        else
        {
            return new RedisNonTracingConnectionFactory(applicationName, redisConfiguration);
        }
    }

    @Provides
    @Named("redisClient")
    public UnifiedJedis getRedisDatabase(ConnectionFactory connectionFactory)
    {
        return connectionFactory.getDatabase();
    }

    @Provides
    @Singleton
    @Named("transactionMode")
    Boolean getTransactionsMode()
    {
        return false;
    }
}
