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

package org.finos.legend.depot.store.notifications.store.redis;

import org.finos.legend.depot.store.notifications.store.api.NotificationsStore;
import redis.clients.jedis.UnifiedJedis;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

public class NotificationsStoreImpl implements NotificationsStore
{
    private final UnifiedJedis redisClient;

    @Inject
    public NotificationsStoreImpl(@Named("redisClient") UnifiedJedis redisClient)
    {
        this.redisClient = redisClient;
    }

    @Override
    public List<String> createIndexes()
    {
        List<String> results = new ArrayList<>();
        results.add(NotificationsRedis.buildIndexes(redisClient));
        results.add(NotificationsQueueRedis.buildIndexes(redisClient));
        return results;
    }
}
