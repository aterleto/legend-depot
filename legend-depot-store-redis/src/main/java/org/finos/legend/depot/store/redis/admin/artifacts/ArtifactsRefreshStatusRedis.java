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

package org.finos.legend.depot.store.redis.admin.artifacts;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.depot.store.admin.api.artifacts.RefreshStatusStore;
import org.finos.legend.depot.store.admin.domain.artifacts.RefreshStatus;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.aggr.AggregationBuilder;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;


public class ArtifactsRefreshStatusRedis extends BaseRedis<RefreshStatus> implements RefreshStatusStore
{
    public static final String COLLECTION = "artifacts-refresh-status";

    private static final String PARENT_EVENT = "parentEventId";
    private static final String PARENT_EVENT_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + PARENT_EVENT;
    private static final String EVENT_ID = "eventId";
    private static final String EVENT_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + EVENT_ID;


    @Inject
    public ArtifactsRefreshStatusRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, RefreshStatus.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableTagField("$." + EVENT_ID, false).as(EVENT_ID)
                .addSortableTagField("$." + PARENT_EVENT, false).as(PARENT_EVENT);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(RefreshStatus data) {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId());
        return sb.toString();
    }

    @Override
    protected Query getKeyFilter(RefreshStatus storeStatus)
    {
        return new Query(getArtifactAndVersionFilter(new StringBuffer(), storeStatus.getGroupId(),
                         storeStatus.getArtifactId(), storeStatus.getVersionId()).toString());
    }

    @Override
    protected void validateNewData(RefreshStatus data)
    {
        //no specific validation
    }

    @Override
    public RefreshStatus createOrUpdate(RefreshStatus data)
    {
        throw new UnsupportedOperationException("createOrUpdate Not supported for refresh status class");
    }

    @Override
    public List<RefreshStatus> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public void insert(RefreshStatus status) {
        insert(getKey(status), true, false, status);
    }

    @Override
    public List<RefreshStatus> find(String groupId, String artifactId, String versionId, String eventId, String parentEventId)
    {
        StringBuffer query = new StringBuffer();

        if (artifactId != null)
        {
            appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, artifactId);
        }
        if (versionId != null)
        {
            appendQueryTagEqualCondition(query, VERSION_ID_TAG, versionId);
        }
        if (eventId != null)
        {
            appendQueryTagEqualCondition(query, EVENT_ID_TAG, eventId);
        }
        if (parentEventId != null)
        {
            appendQueryTagEqualCondition(query, PARENT_EVENT_TAG, parentEventId);
        }

        if (groupId != null)
        {
            appendQueryTagEqualCondition(query, GROUP_ID_TAG, groupId);
            return findAndConvert(COLLECTION, new Query(query.toString()));
        }
        else
        {
            return findAndConvert(COLLECTION, new AggregationBuilder(query.toString())
                    .filter("exists(" + GROUP_ID_TAG + ")"));
        }
    }

    @Override
    public Optional<RefreshStatus> get(String groupId, String artifactId, String versionId)
    {
        return findOneAndConvert(COLLECTION,
                new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString()));
    }

    @Override
    public void delete(String groupId, String artifactId, String versionId)
    {
        Query query = new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        deleteByQuery(COLLECTION, query);
    }

}
