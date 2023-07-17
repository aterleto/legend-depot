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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.depot.domain.notifications.MetadataNotification;
import org.finos.legend.depot.store.notifications.api.Queue;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.SortedField;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class NotificationsQueueRedis extends BaseRedis<MetadataNotification> implements Queue
{
    static final String COLLECTION = "notifications_queue";

    private static final String EVENT_PRIORITY = "eventPriority";
    private static final String EVENT_PRIORITY_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + EVENT_PRIORITY;

    @Inject
    public NotificationsQueueRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, MetadataNotification.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + ID, false).as(ID)
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableTagField("$." + EVENT_PRIORITY, false).as(EVENT_PRIORITY)
                .addSortableNumericField("$." + CREATED).as(CREATED);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(MetadataNotification data) {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId()) .append(REDIS_KEY_DELIMITER);
        sb.append(System.currentTimeMillis());
        return sb.toString(); // unique compound key
    }

    @Override
    protected Query getKeyFilter(MetadataNotification data)
    {
        StringBuffer query = new StringBuffer();
        if (data.getEventId() != null) {
            appendQueryTagEqualCondition(query, ID_TAG, data.getEventId());
        }
        else
        {
            getArtifactAndVersionFilter(query, data.getGroupId(), data.getArtifactId(), data.getVersionId());
        }

        return new Query(query.toString());
    }

    @Override
    protected void validateNewData(MetadataNotification data)
    {
        //no specific validation
    }

    @Override
    public long size()
    {
        return count(COLLECTION, new Query(REDIS_QUERY_WILDCARD));
    }

    public String push(MetadataNotification event)
    {
        MetadataNotification result = createOrUpdate(COLLECTION, true, true, event);
        if (result.getEventId() == null)
        {
            createOrUpdate(COLLECTION, true, true, result.setEventId(result.getId()));
        }
        return result.getEventId();
    }

    public List<MetadataNotification> pullAll()
    {
        List<MetadataNotification> nextEvents = new ArrayList<>();
        findAll(COLLECTION).forEach(document ->
        {
            long deletedCount = deleteByKey(document.getId());
            if (deletedCount != 0)
            { //todo: if it errors, it will get stuck in the queue?
                nextEvents.add(document);
            }
        });
        return nextEvents;
    }

    @Override
    public Optional<MetadataNotification> getFirstInQueue()
    {
        List<MetadataNotification> results = findAndConvert(COLLECTION, new AggregationBuilder(REDIS_QUERY_WILDCARD).filter("exists(" + ID_TAG + ")")
                .sortBy(SortedField.asc(EVENT_PRIORITY_TAG), SortedField.asc(CREATED_TAG)));

        if (results != null && !results.isEmpty())
        {
            return Optional.of(results.get(0));
        }
        return Optional.empty();
    }

    @Override
    public Optional<MetadataNotification> get(String eventId)
    {
        return findOneAndConvert(COLLECTION, new Query(appendQueryTagEqualCondition(new StringBuffer(), ID_TAG, eventId).toString()));
    }

    public List<MetadataNotification> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public long deleteAll()
    {
        return deleteByAggregation(COLLECTION, new AggregationBuilder(REDIS_QUERY_WILDCARD).load(ID)
                .filter("exists(" + GROUP_ID_TAG + ")"));
    }

}