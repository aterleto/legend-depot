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

package org.finos.legend.depot.store.redis.admin.schedules;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.depot.store.admin.api.schedules.ScheduleInstancesStore;
import org.finos.legend.depot.store.admin.domain.schedules.ScheduleInstance;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;


public class ScheduleInstancesRedis extends BaseRedis<ScheduleInstance> implements ScheduleInstancesStore
{
    public static final String COLLECTION = "schedule-instances";

    public static final String SCHEDULE = "schedule";
    private static final String SCHEDULE_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + SCHEDULE;

    @Inject
    public ScheduleInstancesRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, ScheduleInstance.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + ID, false).as(ID)
                .addSortableTagField("$." + SCHEDULE, false).as(SCHEDULE);
        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(ScheduleInstance data)
    {
        StringBuffer sb = new StringBuffer(32);
        sb.append(COLLECTION).append(REDIS_KEY_DELIMITER);
        sb.append(data.getSchedule()).append(REDIS_KEY_DELIMITER);
        sb.append(System.currentTimeMillis());
        return sb.toString();
    }

    @Override
    protected void validateNewData(ScheduleInstance data)
    {
        //no specific validation
    }

    @Override
    protected Query getKeyFilter(ScheduleInstance data)
    {
        return new Query(appendQueryTagEqualCondition(new StringBuffer(), SCHEDULE_TAG, data.getSchedule()).toString());
    }

    @Override
    public List<ScheduleInstance> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public void delete(String instanceId)
    {
        deleteByKey(instanceId);
    }

    @Override
    public List<ScheduleInstance> find(String scheduleName)
    {
        return findAndConvert(COLLECTION, new Query(appendQueryTagEqualCondition(
                new StringBuffer(), SCHEDULE_TAG, scheduleName).toString()));
    }

    @Override
    public void insert(ScheduleInstance instance)
    {
        if (instance == null)
        {
            return;
        }

        // isIdRequired is set to true to avoid upstream changes to the getId() method which is used by MongoDB
        // Once MongoDB is no longer supported, this can be removed since Redis does require internal ID fields
        insert(getKey(instance), false, true, instance);
    }

}
