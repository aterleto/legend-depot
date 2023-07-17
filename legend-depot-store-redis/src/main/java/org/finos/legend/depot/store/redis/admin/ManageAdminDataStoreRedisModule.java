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

import org.finos.legend.depot.store.admin.api.artifacts.ArtifactsFilesStore;
import org.finos.legend.depot.store.admin.api.artifacts.RefreshStatusStore;
import org.finos.legend.depot.store.admin.api.metrics.StorageMetrics;
import org.finos.legend.depot.store.redis.admin.artifacts.ArtifactsFilesRedis;
import org.finos.legend.depot.store.redis.admin.artifacts.ArtifactsRefreshStatusRedis;
import org.finos.legend.depot.store.redis.admin.metrics.StorageMetricsHandler;
import org.finos.legend.depot.store.redis.resources.StoreAdministrationResource;

public class ManageAdminDataStoreRedisModule extends AdminDataStoreRedisModule
{
  
    @Override
    protected void configure()
    {
        super.configure();
        bind(StoreAdministrationResource.class);
        bind(ArtifactsFilesStore.class).to(ArtifactsFilesRedis.class);
        bind(RefreshStatusStore.class).to(ArtifactsRefreshStatusRedis.class);
        bind(RedisAdminStore.class);
        bind(StorageMetrics.class).to(StorageMetricsHandler.class);

        expose(StoreAdministrationResource.class);
        expose(RefreshStatusStore.class);
        expose(ArtifactsFilesStore.class);
        expose(StorageMetrics.class);
        expose(RedisAdminStore.class);
    }
}
