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

package org.finos.legend.depot.store.redis.generation.file;

import org.apache.commons.lang3.StringUtils;
import org.finos.legend.depot.domain.generation.file.StoredFileGeneration;
import org.finos.legend.depot.store.api.generation.file.FileGenerations;
import org.finos.legend.depot.store.api.generation.file.UpdateFileGenerations;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;


public class FileGenerationsRedis extends BaseRedis<StoredFileGeneration> implements FileGenerations, UpdateFileGenerations
{
    public static final String COLLECTION = "file-generations";

    public static final String FILE_PATH = "file_path";
    private static final String FILE_PATH_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + FILE_PATH;
    private static final String GENERATION_PATH = "path";
    private static final String GENERATION_PATH_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + GENERATION_PATH;
    private static final String GENERATION_TYPE = "type";
    private static final String GENERATION_TYPE_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + GENERATION_TYPE;

    @Inject
    public FileGenerationsRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, StoredFileGeneration.class);
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableTagField("$." + GENERATION_PATH, false).as(GENERATION_PATH)
                .addSortableTagField("$." + GENERATION_TYPE, false).as(GENERATION_TYPE)
                .addSortableTagField("$." + StringUtils.replace(FILE_PATH, "_", "."), false).as(FILE_PATH);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(StoredFileGeneration data) {
        StringBuffer sb = new StringBuffer(64);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getFile().getPath());
        return sb.toString(); // unique compound key
    }

    @Override
    public List<StoredFileGeneration> getAll()
    {
        return findAll(COLLECTION);
    }

    @Override
    protected Query getKeyFilter(StoredFileGeneration data)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, data.getGroupId(), data.getArtifactId(), data.getVersionId());
        appendQueryTagEqualCondition(query, FILE_PATH_TAG, data.getFile().getPath());

        return new Query(query.toString());
    }

    @Override
    protected void validateNewData(StoredFileGeneration data)
    {
        //no specific validation
    }

    @Override
    public List<StoredFileGeneration> find(String groupId, String artifactId, String versionId)
    {
        Query query = new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString());
        return findAndConvert(COLLECTION, query);
    }

    @Override
    public List<StoredFileGeneration> findByElementPath(String groupId, String artifactId, String versionId, String generationPath)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, GENERATION_PATH_TAG, generationPath);

        return findAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public Optional<StoredFileGeneration> findByFilePath(String groupId, String artifactId, String versionId, String filePath)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, FILE_PATH_TAG, filePath);

        return findOneAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public List<StoredFileGeneration> findByType(String groupId, String artifactId, String versionId, String type)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, GENERATION_TYPE_TAG, type);

        return findAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public Optional<StoredFileGeneration> get(String groupId, String artifactId, String versionId, String generationFilePath)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, FILE_PATH_TAG, generationFilePath);

        return findOneAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public StoredFileGeneration createOrUpdate(StoredFileGeneration detail) {
        return createOrUpdate(COLLECTION, true, false, detail);
    }

    @Override
    public long delete(String groupId, String artifactId, String versionId)
    {
        Query query = new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString())
                .setNoContent(); // Returns only the ID to reduce overhead

        return deleteByQuery(COLLECTION, query);
    }

}
