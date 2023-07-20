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
import org.finos.legend.depot.domain.api.status.MetadataEventStatus;
import org.finos.legend.depot.domain.notifications.MetadataNotification;
import org.finos.legend.depot.store.notifications.api.Notifications;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.finos.legend.depot.domain.DatesHandler.toTime;


public class NotificationsRedis extends BaseRedis<MetadataNotification> implements Notifications
{
    public static final String COLLECTION = "notifications";

    private static final String EVENT_ID = "eventId";
    private static final String EVENT_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + EVENT_ID;
    private static final String PARENT_EVENT = "parentEventId";
    private static final String PARENT_EVENT_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + PARENT_EVENT;
    private static final String RESPONSE_STATUS = "status";
    private static final String RESPONSE_STATUS_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + RESPONSE_STATUS;

    @Inject
    public NotificationsRedis(@Named("redisClient") UnifiedJedis redisClient)
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
                .addSortableTagField("$." + EVENT_ID, false).as(EVENT_ID)
                .addSortableTagField("$." + PARENT_EVENT, false).as(PARENT_EVENT)
                .addSortableTagField("$." + RESPONSE_STATUS, false).as(RESPONSE_STATUS)
                .addSortableNumericField("$." + UPDATED).as(UPDATED);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(MetadataNotification data)
    {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION).append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()).append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()).append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId());
        return sb.toString(); // unique compound key
    }

    @Override
    protected Query getKeyFilter(MetadataNotification data)
    {
        StringBuffer query = new StringBuffer();
        if (data.getEventId() != null)
        {
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
    public List<MetadataNotification> getAll()
    {
        return findAll(COLLECTION);
    }

    public Optional<MetadataNotification> get(String eventId)
    {
        return findOneAndConvert(COLLECTION, new Query(appendQueryTagEqualCondition(new StringBuffer(), EVENT_ID_TAG, eventId).toString()));
    }

    @Override
    public List<MetadataNotification> find(String groupId, String artifactId, String version, String eventId,
                                           String parentEventId, Boolean success, LocalDateTime fromDate, LocalDateTime toDate)
    {
        StringBuffer query = new StringBuffer();

        LocalDateTime to = toDate != null ? toDate : LocalDateTime.now();
        appendQueryTagLessThanOrEqualToCondition(query, UPDATED_TAG, toTime(to));

        if (fromDate != null)
        {
            appendQueryTagGreaterThanOrEqualToCondition(query, UPDATED_TAG, toTime(fromDate));
        }
        if (groupId != null)
        {
            appendQueryTagEqualCondition(query, GROUP_ID_TAG, groupId);
        }
        if (artifactId != null)
        {
            appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, artifactId);
        }
        if (version != null)
        {
            appendQueryTagEqualCondition(query, VERSION_ID_TAG, version);
        }
        if (eventId != null)
        {
            appendQueryTagEqualCondition(query, EVENT_ID_TAG, eventId);
        }
        if (parentEventId != null)
        {
            appendQueryTagEqualCondition(query, PARENT_EVENT_TAG, parentEventId);
        }
        if (success != null)
        {
            appendQueryTagEqualCondition(query, RESPONSE_STATUS_TAG, success ?
                    MetadataEventStatus.SUCCESS.name() : MetadataEventStatus.FAILED.name());
        }

        return findAndConvert(COLLECTION, new Query(query.toString()).setSortBy(UPDATED, false));
    }

    @Override
    public MetadataNotification createOrUpdate(MetadataNotification metadataEvent)
    {
        return createOrUpdate(COLLECTION, true, true, metadataEvent);
    }

    @Override
    public void delete(String id)
    {
       deleteByKey(id);
    }

}