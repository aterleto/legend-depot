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

package org.finos.legend.depot.store.redis.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.finos.legend.depot.store.redis.admin.artifacts.ArtifactsFilesRedis;
import org.finos.legend.depot.store.redis.admin.artifacts.ArtifactsRefreshStatusRedis;
import org.finos.legend.depot.store.redis.admin.metrics.QueryMetricsRedis;
import org.finos.legend.depot.store.redis.admin.schedules.ScheduleInstancesRedis;
import org.finos.legend.depot.store.redis.admin.schedules.SchedulesRedis;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import org.finos.legend.depot.store.redis.entities.EntitiesRedis;
import org.finos.legend.depot.store.redis.generation.file.FileGenerationsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsRedis;
import org.finos.legend.depot.store.redis.projects.ProjectsVersionsRedis;
import redis.clients.jedis.UnifiedJedis;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class RedisAdminStore
{
    private final UnifiedJedis redisClient;
    private static List<String> collectionNameList = new ArrayList<>();

    static {
        collectionNameList.add(ProjectsRedis.COLLECTION);
        collectionNameList.add(ProjectsVersionsRedis.COLLECTION);
        collectionNameList.add(EntitiesRedis.COLLECTION);
        collectionNameList.add(FileGenerationsRedis.COLLECTION);
        collectionNameList.add(ArtifactsFilesRedis.COLLECTION);
        collectionNameList.add(ArtifactsRefreshStatusRedis.COLLECTION);
        collectionNameList.add(SchedulesRedis.COLLECTION);
        collectionNameList.add(ScheduleInstancesRedis.COLLECTION);
        collectionNameList.add(QueryMetricsRedis.COLLECTION);
    }

    @Inject
    public RedisAdminStore(@Named("redisClient") UnifiedJedis redisClient)
    {
        this.redisClient = redisClient;
    }


    public void deleteCollection(String collectionName)
    {
        //mongoDatabase.getCollection(collectionId).drop(); // TODO possibly implement SCAN and DEL
    }

    public List<String> getAllCollections()
    {
        return collectionNameList;
    }

    public List<String> getAllIndexes()
    {
        return redisClient.ftList();
    }

    public void deleteIndex(String collectionName)
    {
        redisClient.ftDropIndex(collectionName + BaseRedis.REDIS_QUERY_INDEX_SUFFIX);
    }

    public List<String> createIndexes()
    {
        List<String> results = new ArrayList<>();
        results.add(ProjectsRedis.buildIndexes(redisClient));
        results.add(ProjectsVersionsRedis.buildIndexes(redisClient));
        results.add(EntitiesRedis.buildIndexes(redisClient));
        results.add(FileGenerationsRedis.buildIndexes(redisClient));
        results.add(ArtifactsFilesRedis.buildIndexes(redisClient));
        results.add(ArtifactsRefreshStatusRedis.buildIndexes(redisClient));
        results.add(SchedulesRedis.buildIndexes(redisClient));
        results.add(ScheduleInstancesRedis.buildIndexes(redisClient));
        results.add(QueryMetricsRedis.buildIndexes(redisClient));
        return results;
    }

    /*
    public Document runCommand(Document document)
    {
        return mongoDatabase.runCommand(document);
    }

    public List<Document> runPipeline(String collectionName, List<Document> pipeline)
    {
        List<Document> documents = new ArrayList<>();
        mongoDatabase.getCollection(collectionName).aggregate(pipeline).forEach((Consumer<? super Document>) doc ->
        {
            documents.add(doc);
        });
        return documents;
    }

    public List<Document> runPipeline(String collectionName, String jsonPipeline) throws JsonProcessingException
    {
        List<Document> pipeline =  new ObjectMapper().readValue(jsonPipeline, new TypeReference<List<Document>>(){});;
         return runPipeline(collectionName,pipeline);
    }

    public String getName()
    {
        //return mongoDatabase.getName(); //TODO
        return "";
    }
    */

}
