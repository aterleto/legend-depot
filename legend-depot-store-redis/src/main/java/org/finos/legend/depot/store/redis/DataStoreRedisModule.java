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

package org.finos.legend.depot.store.redis;

import org.finos.legend.depot.store.api.entities.Entities;
import org.finos.legend.depot.store.api.generation.file.FileGenerations;
import org.finos.legend.depot.store.api.projects.Projects;
import org.finos.legend.depot.store.api.projects.ProjectsVersions;
import org.finos.legend.depot.store.redis.core.RedisStoreConnectionModule;
import org.finos.legend.depot.store.redis.entities.EntitiesRedis;
import org.finos.legend.depot.store.redis.generation.file.FileGenerationsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsVersionsRedis;

public class DataStoreRedisModule extends RedisStoreConnectionModule
{
    @Override
    protected void configure()
    {
        super.configure();
        bind(Projects.class).to(ProjectsRedis.class);
        bind(ProjectsVersions.class).to(ProjectsVersionsRedis.class);
        bind(Entities.class).to(EntitiesRedis.class);
        bind(FileGenerations.class).to(FileGenerationsRedis.class);

        expose(Entities.class);
        expose(ProjectsVersions.class);
        expose(FileGenerations.class);
        expose(Projects.class);
    }
}
