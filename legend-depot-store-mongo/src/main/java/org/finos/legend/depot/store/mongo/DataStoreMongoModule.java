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

package org.finos.legend.depot.store.mongo;

import org.finos.legend.depot.store.api.entities.Entities;
import org.finos.legend.depot.store.api.generation.file.FileGenerations;
import org.finos.legend.depot.store.api.projects.Projects;
import org.finos.legend.depot.store.mongo.core.MongoStoreConnectionModule;
import org.finos.legend.depot.store.mongo.entities.EntitiesMongo;
import org.finos.legend.depot.store.mongo.generation.file.FileGenerationsMongo;
import org.finos.legend.depot.store.mongo.projects.ProjectsMongo;

public class DataStoreMongoModule extends MongoStoreConnectionModule
{
    @Override
    protected void configure()
    {
        super.configure();
        bind(Projects.class).to(ProjectsMongo.class);
        bind(Entities.class).to(EntitiesMongo.class);
        bind(FileGenerations.class).to(FileGenerationsMongo.class);

        expose(Entities.class);
        expose(FileGenerations.class);
        expose(Projects.class);
    }
}