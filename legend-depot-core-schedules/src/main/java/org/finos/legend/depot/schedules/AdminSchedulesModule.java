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

package org.finos.legend.depot.schedules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.finos.legend.depot.schedules.resources.SchedulesResource;
import org.finos.legend.depot.schedules.services.SchedulesFactory;

import javax.inject.Named;

public class AdminSchedulesModule extends SchedulesModule
{
    @Override
    protected void configure()
    {
        super.configure();
        bind(SchedulesResource.class);
        expose(SchedulesResource.class);
    }

    @Provides
    @Singleton
    @Named("register-housekeeper")
    public boolean registerHouseKeeper(SchedulesFactory schedulesFactory)
    {
        schedulesFactory.registerHouseKeeper();
        return true;
    }

}
