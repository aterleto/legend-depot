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
import org.finos.legend.depot.domain.api.MetadataEventResponse;
import org.finos.legend.depot.domain.project.ProjectValidator;
import org.finos.legend.depot.domain.project.StoreProjectData;
import org.finos.legend.depot.store.StoreException;
import org.finos.legend.depot.store.api.projects.Projects;
import org.finos.legend.depot.store.api.projects.UpdateProjects;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;


public class ProjectsRedis extends BaseRedis<StoreProjectData> implements Projects, UpdateProjects
{
    public static final String COLLECTION = "project-configurations";

    public static final String PROJECT_ID = "projectId";
    private static final String PROJECT_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + PROJECT_ID;

    @Inject
    public ProjectsRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, StoreProjectData.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + PROJECT_ID, false).as(PROJECT_ID);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(StoreProjectData data)
    {
        StringBuffer sb = new StringBuffer(32);
        sb.append(COLLECTION).append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()).append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId());
        return sb.toString(); // unique compound key
    }

    @Override
    protected Query getKeyFilter(StoreProjectData data)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, GROUP_ID_TAG, data.getGroupId());
        appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, data.getArtifactId());

        return new Query(query.toString());
    }

    @Override
    protected void validateNewData(StoreProjectData data)
    {
        if (!ProjectValidator.isValid(data))
        {
            throw new IllegalArgumentException(String.format("invalid project [%s] or invalid groupId [%s] or artifactId [%s]",data.getProjectId(),data.getGroupId(),data.getArtifactId()));
        }

        Optional<StoreProjectData> projectData = find(data.getGroupId(), data.getArtifactId());
        if (projectData.isPresent() && (!data.getProjectId().equals(projectData.get().getProjectId())))
        {
            throw new StoreException(String.format("Duplicate coordinates: Different project %s its already registered with this coordinates %s-%s", projectData.get().getProjectId(), data.getGroupId(), data.getArtifactId()));
        }
    }

    @Override
    public List<StoreProjectData> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    public List<StoreProjectData> getProjects(int page, int pageSize)
    {
        return findAllByPage(COLLECTION, page, pageSize);
    }

    @Override
    public List<StoreProjectData> findByProjectId(String projectId)
    {
        return findAndConvert(COLLECTION, new Query(
                appendQueryTagEqualCondition(new StringBuffer(), PROJECT_ID_TAG, projectId).toString()));
    }

    @Override
    public Optional<StoreProjectData> find(String groupId, String artifactId)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, GROUP_ID_TAG, groupId);
        appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, artifactId);

        return findOneAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public StoreProjectData createOrUpdate(StoreProjectData projectCoordinates)
    {
        return createOrUpdate(COLLECTION, true, false, projectCoordinates);
    }

    @Override
    public MetadataEventResponse delete(String groupId, String artifactId)
    {
        Query query = new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        deleteByQuery(COLLECTION, query);

        return new MetadataEventResponse();
    }

}