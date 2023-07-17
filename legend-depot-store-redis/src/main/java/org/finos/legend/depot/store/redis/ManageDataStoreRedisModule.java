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

import org.finos.legend.depot.store.api.entities.UpdateEntities;
import org.finos.legend.depot.store.api.generation.file.UpdateFileGenerations;
import org.finos.legend.depot.store.api.projects.UpdateProjects;
import org.finos.legend.depot.store.api.projects.UpdateProjectsVersions;
import org.finos.legend.depot.store.redis.entities.EntitiesRedis;
import org.finos.legend.depot.store.redis.generation.file.FileGenerationsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsVersionsRedis;

public class ManageDataStoreRedisModule extends DataStoreRedisModule
{
    @Override
    protected void configure()
    {
        super.configure();
        bind(UpdateEntities.class).to(EntitiesRedis.class);
        bind(UpdateProjects.class).to(ProjectsRedis.class);
        bind(UpdateProjectsVersions.class).to(ProjectsVersionsRedis.class);
        bind(UpdateFileGenerations.class).to(FileGenerationsRedis.class);

        expose(UpdateEntities.class);
        expose(UpdateProjectsVersions.class);
        expose(UpdateFileGenerations.class);
        expose(UpdateProjects.class);
    }
}
