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

package org.finos.legend.depot.store.redis.projects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.finos.legend.depot.domain.CoordinateValidator;
import org.finos.legend.depot.domain.api.MetadataEventResponse;
import org.finos.legend.depot.domain.project.StoreProjectVersionData;
import org.finos.legend.depot.domain.version.VersionValidator;
import org.finos.legend.depot.store.api.projects.ProjectsVersions;
import org.finos.legend.depot.store.api.projects.UpdateProjectsVersions;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.SearchResult;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;


public class ProjectsVersionsRedis extends BaseRedis<StoreProjectVersionData> implements ProjectsVersions, UpdateProjectsVersions
{
    public static final String COLLECTION = "versions";

    private static final String VERSION_DATA_EXCLUDED = "versionData_excluded";
    private static final String VERSION_DATA_EXCLUDED_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + VERSION_DATA_EXCLUDED;

    @Inject
    public ProjectsVersionsRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, StoreProjectVersionData.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableTagField("$." + StringUtils.replace(VERSION_DATA_EXCLUDED, "_", "."), false).as(VERSION_DATA_EXCLUDED);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(StoreProjectVersionData data) {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId());
        return sb.toString(); // unique compound key
    }

    @Override
    public List<StoreProjectVersionData> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public List<StoreProjectVersionData> find(String groupId, String artifactId)
    {
        return findAndConvert(COLLECTION, new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString()));
    }

    @Override
    public Optional<StoreProjectVersionData> find(String groupId, String artifactId, String versionId)
    {
        if (versionId == null || versionId.isEmpty())
        {
            throw new IllegalArgumentException("cannot find project version, versionId cannot be null");
        }

        StringBuffer query = getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId);
        return findOneAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public List<StoreProjectVersionData> findVersion(Boolean excluded)
    {
        StringBuffer query = appendQueryTagEqualCondition(new StringBuffer(), VERSION_DATA_EXCLUDED_TAG, excluded);
        return findAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public long getVersionCount(String groupId, String artifactId)
    {
        Query query = new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString())
                .limit(0,0); // Returns only the ID to reduce overhead

        SearchResult result = redisClient.ftSearch(COLLECTION + REDIS_QUERY_INDEX_SUFFIX, query);
        if (result == null)
            return 0;

        return result.getTotalResults();
    }

    @Override
    public StoreProjectVersionData createOrUpdate(StoreProjectVersionData projectVersionData) {
        return createOrUpdate(COLLECTION, true, false, projectVersionData);
    }

    @Override
    public MetadataEventResponse delete(String groupId, String artifactId)
    {
        Query query = new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        deleteByQuery(COLLECTION, query);

        return new MetadataEventResponse();
    }

    @Override
    public MetadataEventResponse deleteByVersionId(String groupId, String artifactId, String versionId)
    {
        Query query = new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        deleteByQuery(COLLECTION, query);

        return new MetadataEventResponse();
    }

    @Override
    protected Query getKeyFilter(StoreProjectVersionData data)
    {
        return new Query(getArtifactAndVersionFilter(new StringBuffer(), data.getGroupId(), data.getArtifactId(), data.getVersionId()).toString());
    }

    @Override
    protected void validateNewData(StoreProjectVersionData data)
    {
        /* //TODO remove after tests
        if (!CoordinateValidator.isValidGroupId(data.getGroupId()) || !CoordinateValidator.isValidArtifactId(data.getArtifactId()))
        {
            throw new IllegalArgumentException(String.format("invalid groupId [%s] or artifactId [%s]",data.getGroupId(),data.getArtifactId()));
        }
        if (!VersionValidator.isValid(data.getVersionId()))
        {
            throw new IllegalArgumentException(String.format("invalid versionId [%s]",data.getVersionId()));
        }
        */
    }

}