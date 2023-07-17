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

package org.finos.legend.depot.store.redis.admin;

import com.google.inject.PrivateModule;
import org.finos.legend.depot.store.admin.api.metrics.QueryMetricsStore;
import org.finos.legend.depot.store.admin.api.schedules.ScheduleInstancesStore;
import org.finos.legend.depot.store.admin.api.schedules.SchedulesStore;
import org.finos.legend.depot.store.redis.admin.metrics.QueryMetricsRedis;
import org.finos.legend.depot.store.redis.admin.schedules.ScheduleInstancesRedis;
import org.finos.legend.depot.store.redis.admin.schedules.SchedulesRedis;

public class AdminDataStoreRedisModule extends PrivateModule
{
    @Override
    protected void configure()
    {
        bind(SchedulesStore.class).to(SchedulesRedis.class);
        bind(ScheduleInstancesStore.class).to(ScheduleInstancesRedis.class);
        bind(QueryMetricsStore.class).to(QueryMetricsRedis.class);

        expose(QueryMetricsStore.class);
        expose(SchedulesStore.class);
        expose(ScheduleInstancesStore.class);
    }
}
