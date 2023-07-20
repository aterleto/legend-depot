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
import org.finos.legend.depot.store.admin.api.artifacts.ArtifactsFilesStore;
import org.finos.legend.depot.store.admin.domain.artifacts.ArtifactFile;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;


public class ArtifactsFilesRedis extends BaseRedis<ArtifactFile> implements ArtifactsFilesStore
{
    public static final String COLLECTION = "artifacts-files";

    private static final String PATH = "path";
    private static final String PATH_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + PATH;

    @Inject
    public ArtifactsFilesRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        super(redisClient, ArtifactFile.class, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY));
        buildIndexes(redisClient);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema().addSortableTagField("$." + PATH, false).as(PATH);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(ArtifactFile data)
    {
        return COLLECTION + REDIS_KEY_DELIMITER + data.getPath(); // unique
    }

    @Override
    protected Query getKeyFilter(ArtifactFile data)
    {
        return new Query(appendQueryTagEqualCondition(new StringBuffer(), PATH_TAG, data.getPath()).toString());
    }

    @Override
    protected void validateNewData(ArtifactFile data)
    {
        //no specific validation
    }

    @Override
    public ArtifactFile createOrUpdate(ArtifactFile detail)
    {
        return createOrUpdate(COLLECTION, true, false, detail);
    }

    @Override
    public Optional<ArtifactFile> find(String path)
    {
        return findOneAndConvert(COLLECTION,
                new Query(appendQueryTagEqualCondition(new StringBuffer(), PATH_TAG, path).toString()));
    }
}
