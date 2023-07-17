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


package org.finos.legend.depot.store.redis.admin.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.depot.domain.project.ProjectVersion;
import org.finos.legend.depot.store.admin.api.metrics.QueryMetricsStore;
import org.finos.legend.depot.store.admin.domain.metrics.VersionQueryMetric;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.Path;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;
import redis.clients.jedis.search.aggr.Reducers;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;


public class QueryMetricsRedis extends BaseRedis<VersionQueryMetric> implements QueryMetricsStore
{

    public static final String COLLECTION = "query-metrics";

    private static final String COORDINATE = "coordinate";
    private static final String COORDINATE_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + COORDINATE;
    private static final String LAST_QUERY_TIME = "lastQueryTime";
    private static final String LAST_QUERY_TIME_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + LAST_QUERY_TIME;

    private static final String REDIS_APPLY_CONCATENATION = "format('%s:%s:%s', " + GROUP_ID_TAG + ", " +
            ARTIFACT_ID_TAG + ", " + VERSION_ID_TAG + ")";

    @Inject
    public QueryMetricsRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, VersionQueryMetric.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableNumericField("$." + LAST_QUERY_TIME).as(LAST_QUERY_TIME);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(VersionQueryMetric data) {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getLastQueryTime().getTime());
        return sb.toString(); // unique compound key
    }

    @Override
    public List<VersionQueryMetric> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public List<ProjectVersion> getAllStoredEntitiesCoordinates()
    {
        AggregationBuilder aggregation = new AggregationBuilder(REDIS_QUERY_WILDCARD)
                .apply(REDIS_APPLY_CONCATENATION, COORDINATE)
                .groupBy(COORDINATE_TAG, Reducers.count());

        AggregationResult resultSet = redisClient.ftAggregate(COLLECTION + REDIS_QUERY_INDEX_SUFFIX, aggregation);

        List<ProjectVersion> projectVersionList = new ArrayList<>();

        List<Map<String, Object>> applyAggregateResults = resultSet.getResults();
        if (applyAggregateResults != null && !applyAggregateResults.isEmpty())
        {
            for (Map<String, Object> result : applyAggregateResults)
            {
                if (result != null && result.containsKey(COORDINATE))
                {
                    StringTokenizer tokenizer = new StringTokenizer(String.valueOf(result.get(COORDINATE)), ":");
                    projectVersionList.add(new ProjectVersion(tokenizer.nextToken(), tokenizer.nextToken(), tokenizer.nextToken()));
                }
            }
        }
        return projectVersionList;
    }

    @Override
    public List<VersionQueryMetric> get(String groupId, String artifactId, String versionId)
    {
        return findAndConvert(COLLECTION, new Query(getKeyFilter(groupId, artifactId, versionId).toString()));
    }

    @Override
    public void record(String groupId, String artifactId, String versionId)
    {
        record(new VersionQueryMetric(groupId, artifactId, versionId));
    }

    public void record(VersionQueryMetric metric)
    {
        redisClient.jsonSet(getKey(metric), Path.ROOT_PATH, convertDomainToPropertiesMap(metric));
    }

    @Override
    public long consolidate(VersionQueryMetric metric)
    {
        StringBuffer query = getKeyFilter(metric.getGroupId(), metric.getArtifactId(), metric.getVersionId());
        appendQueryTagLessThanCondition(query, LAST_QUERY_TIME_TAG, metric.getLastQueryTime().getTime());

        return deleteByQuery(COLLECTION, new Query(query.toString()).setNoContent());
    }

    @Override
    public List<VersionQueryMetric> findMetricsBefore(Date date)
    {
        return findAndConvert(COLLECTION, new Query(
                appendQueryTagLessThanOrEqualToCondition(new StringBuffer(), LAST_QUERY_TIME_TAG, date.getTime()).toString()));
    }

    @Override
    protected Query getKeyFilter(VersionQueryMetric metric)
    {
        return new Query(getKeyFilter(metric.getGroupId(), metric.getArtifactId(), metric.getVersionId()).toString());
    }

    protected StringBuffer getKeyFilter(String groupId, String artifactId, String versionId)
    {
        return getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId);
    }

    @Override
    protected void validateNewData(VersionQueryMetric data)
    {
        //no specific validation
    }

}
