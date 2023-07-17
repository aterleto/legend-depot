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
import org.finos.legend.depot.store.admin.api.schedules.SchedulesStore;
import org.finos.legend.depot.store.admin.domain.schedules.ScheduleInfo;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;


public class SchedulesRedis extends BaseRedis<ScheduleInfo> implements SchedulesStore
{
    public static final String COLLECTION = "schedules";

    public static final String NAME = "name";
    private static final String NAME_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + NAME;

    @Inject
    public SchedulesRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, ScheduleInfo.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema().addSortableTagField("$." + NAME, false).as(NAME);
        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(ScheduleInfo data) {
        return COLLECTION + REDIS_KEY_DELIMITER + data.getName(); // based on the use of findOne it can be assumed that name is unique
    }

    @Override
    protected void validateNewData(ScheduleInfo data)
    {
        //nothing to validate
    }

    @Override
    protected Query getKeyFilter(ScheduleInfo data)
    {
        return new Query(appendQueryTagEqualCondition(new StringBuffer(), NAME_TAG, data.getName()).toString());
    }

    @Override
    public Optional<ScheduleInfo> get(String name)
    {
        return findOneAndConvert(COLLECTION, new Query(appendQueryTagEqualCondition(new StringBuffer(), NAME_TAG, name).toString()));
    }

    @Override
    public ScheduleInfo createOrUpdate(ScheduleInfo scheduleInfo) {
        return createOrUpdate(COLLECTION, true, false, scheduleInfo);
    }

    @Override
    public List<ScheduleInfo> getAll()
    {
        return super.findAll(COLLECTION);
    }

    @Override
    public void delete(String name)
    {
        Query query = new Query(appendQueryTagEqualCondition(new StringBuffer(), NAME_TAG, name).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        deleteByQuery(COLLECTION, query);
    }

}
