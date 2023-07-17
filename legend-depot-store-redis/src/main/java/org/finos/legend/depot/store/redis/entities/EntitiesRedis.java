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

package org.finos.legend.depot.store.redis.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.finos.legend.depot.domain.CoordinateValidator;
import org.finos.legend.depot.domain.entity.EntityValidationErrors;
import org.finos.legend.depot.domain.entity.StoredEntity;
import org.finos.legend.depot.domain.entity.StoredEntityOverview;
import org.finos.legend.depot.domain.project.ProjectVersion;
import org.finos.legend.depot.domain.status.StoreOperationResult;
import org.finos.legend.depot.domain.version.VersionValidator;
import org.finos.legend.depot.store.api.entities.Entities;
import org.finos.legend.depot.store.api.entities.UpdateEntities;
import org.finos.legend.depot.store.redis.core.BaseRedis;
import org.finos.legend.sdlc.domain.model.entity.Entity;
import org.finos.legend.sdlc.tools.entity.EntityPaths;
import org.slf4j.Logger;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;
import redis.clients.jedis.search.aggr.Reducers;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.finos.legend.depot.domain.DatesHandler.toTime;
import static org.finos.legend.depot.domain.version.VersionValidator.MASTER_SNAPSHOT;


public class EntitiesRedis extends BaseRedis<StoredEntity> implements Entities, UpdateEntities
{
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(EntitiesRedis.class);
    public static final String COLLECTION = "entities";

    public static final String CLASSIFIER_PATH = "classifierPath";
    public static final String ENTITY = "entity";
    public static final String ENTITY_CLASSIFIER_PATH = "entity_classifierPath";
    private static final String ENTITY_CLASSIFIER_PATH_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + ENTITY_CLASSIFIER_PATH;
    public static final String ENTITY_CONTENT = "entity_content";
    public static final String ENTITY_PACKAGE = "entity_content_package";
    private static final String ENTITY_PACKAGE_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + ENTITY_PACKAGE;
    public static final String ENTITY_PATH = "entity_path";
    private static final String ENTITY_PATH_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + ENTITY_PATH;
    public static final String PATH = "path";
    public static final String VERSIONED_ENTITY = "versionedEntity";
    private static final String VERSIONED_ENTITY_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + VERSIONED_ENTITY;

    private static final String REDIS_APPLY_CONCATENATION = "format('%s:%s', " + GROUP_ID_TAG + ", " + ARTIFACT_ID_TAG + ")";

    @Inject
    public EntitiesRedis(@Named("redisClient") UnifiedJedis redisClient, @Named("transactionMode") boolean transactionMode)
    {
        super(redisClient, StoredEntity.class);
        //this.transactionMode = false; // Redis does not support distributed transactions
        buildIndexes(redisClient);
    }

    public EntitiesRedis(@Named("redisClient") UnifiedJedis redisClient)
    {
        this(redisClient, false);
    }

    public static String buildIndexes(UnifiedJedis redisClient)
    {
        Schema schema = new Schema()
                .addSortableTagField("$." + GROUP_ID, false).as(GROUP_ID)
                .addSortableTagField("$." + ARTIFACT_ID, false).as(ARTIFACT_ID)
                .addSortableTagField("$." + VERSION_ID, false).as(VERSION_ID)
                .addSortableTagField("$." + VERSIONED_ENTITY, false).as(VERSIONED_ENTITY)
                .addSortableTagField("$." + StringUtils.replace(ENTITY_PATH, "_", "."), false).as(ENTITY_PATH)
                .addSortableTagField("$." + StringUtils.replace(ENTITY_PACKAGE, "_", "."), false).as(ENTITY_PACKAGE)
                .addSortableTagField("$." + StringUtils.replace(ENTITY_CLASSIFIER_PATH, "_", "."), false).as(ENTITY_CLASSIFIER_PATH);

        return buildIndex(redisClient, COLLECTION, schema);
    }

    @Override
    protected String getKey(StoredEntity data) {
        StringBuffer sb = new StringBuffer(128);
        sb.append(COLLECTION) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getGroupId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getArtifactId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getVersionId()) .append(REDIS_KEY_DELIMITER);
        sb.append(data.getEntity().getPath());
        return sb.toString(); // unique compound key
    }

    @Override
    protected Query getKeyFilter(StoredEntity storedEntity)
    {
        return getEntityPathFilter(storedEntity);
    }


    private Query getEntityPathFilter(StoredEntity storedEntity)
    {
        StringBuffer query = getArtifactAndVersionFilter(new StringBuffer(), storedEntity.getGroupId(), storedEntity.getArtifactId(), storedEntity.getVersionId());
        appendQueryTagEqualCondition(query, ENTITY_PATH_TAG, storedEntity.getEntity().getPath());

        return new Query(query.toString());
    }

    private StringBuffer getEntityPathFilter(StringBuffer query, String groupId, String artifactId, String versionId, String path)
    {
        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, ENTITY_PATH_TAG, path);

        return query;
    }

    protected StringBuffer getArtifactWithVersionsFilter(StringBuffer query, String groupId, String artifactId,
                                                         String versionId, boolean versioned)
    {
        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versioned);

        return query;
    }

    @Override
    protected void validateNewData(StoredEntity data)
    {
        StoreOperationResult result = validateEntity(data);
        if (result.hasErrors())
        {
            throw new IllegalArgumentException("invalid data " + result.getErrors());
        }
    }

    private StoreOperationResult validateEntity(StoredEntity entity)
    {
        StoreOperationResult result = new StoreOperationResult();

        if (!CoordinateValidator.isValidGroupId(entity.getGroupId()))
        {
            result.logError(String.format(EntityValidationErrors.INVALID_GROUP_ID, entity.getGroupId()));
        }
        if (!CoordinateValidator.isValidArtifactId(entity.getArtifactId()))
        {
            result.logError(String.format(EntityValidationErrors.INVALID_ARTIFACT_ID, entity.getArtifactId()));
        }
        if (!VersionValidator.isValid(entity.getVersionId()))
        {
            result.logError(String.format(EntityValidationErrors.INVALID_VERSION_ID, entity.getVersionId()));
        }
        if (!EntityPaths.isValidEntityPath(entity.getEntity().getPath()))
        {
            result.logError(String.format(EntityValidationErrors.INVALID_ENTITY_PATH, entity.getEntity().getPath()));
        }

        return result;
    }

    StoreOperationResult newOrUpdate(StoredEntity entity)
    {
        StoreOperationResult report = validateEntity(entity);
        if (report.hasErrors())
        {
            return report;
        }

        Optional<StoredEntity> optionalResult = findOneAndConvert(COLLECTION, getEntityPathFilter(entity));
        if (optionalResult.isPresent()) {

            StoredEntity storedEntity = optionalResult.get();

            Map<String, Object> existingEntityPropertiesMap = convertDomainToPropertiesMap(storedEntity);
            existingEntityPropertiesMap.put(GROUP_ID, entity.getGroupId());
            existingEntityPropertiesMap.put(ARTIFACT_ID, entity.getArtifactId());
            existingEntityPropertiesMap.put(VERSION_ID, entity.getVersionId());
            existingEntityPropertiesMap.put(VERSIONED_ENTITY, entity.isVersionedEntity());
            existingEntityPropertiesMap.put(ENTITY_PATH, entity.getEntity().getPath());
            existingEntityPropertiesMap.put(ENTITY_CLASSIFIER_PATH, entity.getEntity().getClassifierPath());
            existingEntityPropertiesMap.put(ENTITY_CONTENT, entity.getEntity().getContent());
            existingEntityPropertiesMap.put(UPDATED, toTime(LocalDateTime.now()));

            insert(getKey(storedEntity), false, false, existingEntityPropertiesMap);
            report.addModifiedCount();
        }
        else
        {
            insert(getKey(entity), true, false, entity);
            report.addInsertedCount();
        }

        return report;
    }

    @Override
    public StoreOperationResult createOrUpdate(List<StoredEntity> versionedEntities)
    {
        StoreOperationResult report = new StoreOperationResult();
        for (StoredEntity versionedEntity : versionedEntities)
        {
            report.combine(newOrUpdate(versionedEntity));
        }

        if (report.getInsertedCount() + report.getModifiedCount() != versionedEntities.size())
        {
            report.logError("error creating/updating entities,did not get acknowledgment for all");
        }

        return report;
    }

    @Override
    public Optional<Entity> getEntity(String groupId, String artifactId, String versionId, String path)
    {
        Query query = new Query(getEntityPathFilter(new StringBuffer(), groupId, artifactId, versionId, path).toString());
        return findOneAndConvert(COLLECTION, query).map(StoredEntity::getEntity);
    }

    @Override
    public List<StoredEntity> getStoredEntities(String groupId, String artifactId)
    {
        return findAndConvert(COLLECTION, new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString()));
    }

    @Override
    public List<StoredEntity> getStoredEntities(String groupId, String artifactId, String versionId)
    {
        return findAndConvert(COLLECTION, new Query(getArtifactAndVersionFilter(
                new StringBuffer(), groupId, artifactId, versionId).toString()));
    }

    @Override
    public List<StoredEntity> getStoredEntities(String groupId, String artifactId, String versionId, boolean versioned)
    {
        return findAndConvert(COLLECTION, new Query(getArtifactWithVersionsFilter(
                new StringBuffer(), groupId, artifactId, versionId, versioned).toString()));
    }

    @Override
    public List<Entity> getAllEntities(String groupId, String artifactId, String versionId)
    {
        Query query = new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString());
        return findAndConvert(COLLECTION, query).stream().map(StoredEntity::getEntity).collect(Collectors.toList());
    }


    private List<Entity> getAllEntities(String groupId, String artifactId, String versionId, boolean versioned)
    {
        Query query = new Query(getArtifactWithVersionsFilter(new StringBuffer(), groupId, artifactId, versionId, versioned).toString());
        return findAndConvert(COLLECTION, query).stream().map(StoredEntity::getEntity).collect(Collectors.toList());
    }

    @Override
    public List<Entity> getEntitiesByPackage(String groupId, String artifactId, String versionId, String packageName,
                                             boolean versioned, Set<String> classifierPaths, boolean includeSubPackages)
    {
        StringBuffer query = getArtifactWithVersionsFilter(new StringBuffer(), groupId, artifactId, versionId, versioned);
        if (includeSubPackages)
        {
            appendQueryTagPrefixCondition(query, ENTITY_PACKAGE_TAG, packageName);
        }
        else
        {
            appendQueryTagEqualCondition(query, ENTITY_PACKAGE_TAG, packageName);
        }

        Stream<Entity> entities = findAndConvert(COLLECTION, new Query(query.toString())).stream().map(StoredEntity::getEntity);
        if (classifierPaths != null && !classifierPaths.isEmpty())
        {
            entities = entities.filter(entity -> classifierPaths.contains(entity.getClassifierPath()));
        }
        return entities.collect(Collectors.toList());
    }

    protected List<StoredEntity> transform(boolean summary, List<Document> documents)
    {
        if (!summary)
        {
            return convertDocumentsToDomainList(documents);
        }

        List<StoredEntity> result = new ArrayList<>();
        if (documents == null)
            return result;

        for (Document document : documents) {

            if (document == null || !document.hasProperty(REDIS_JSON_ROOT))
            {
                continue;
            }

            String jsonPayload = String.valueOf(document.get(REDIS_JSON_ROOT));
            if (jsonPayload == null || jsonPayload.isEmpty()) {
                continue;
            }

            try
            {
                Map<String, Object> properties = objectMapper.readValue(jsonPayload, new TypeReference<Map<String, Object>>() {});
                if (properties != null)
                {
                    Map<String, Object> entity = (Map<String, Object> ) properties.get(ENTITY);
                    result.add(new StoredEntityOverview(String.valueOf(properties.get(GROUP_ID)),
                            String.valueOf(properties.get(ARTIFACT_ID)),
                            String.valueOf(properties.get(VERSION_ID)),
                            (boolean) properties.get(VERSIONED_ENTITY),
                            (String) entity.get(PATH),
                            (String) entity.get(CLASSIFIER_PATH)));
                }
            }
            catch (JsonProcessingException jpe) {
                LOGGER.error("error transforming stored entity id:" + COLLECTION + document.getId(), jpe);
                continue;
            }
        }
        return result;
    }

    @Override
    public List<StoredEntity> findReleasedEntitiesByClassifier(String classifier, String search, List<ProjectVersion> projectVersions,
                                                               Integer limit, boolean summary, boolean versioned)
    {
        StringBuffer queryString = new StringBuffer();

        appendQueryTagEqualCondition(queryString, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(queryString, VERSIONED_ENTITY_TAG, versioned);

        if (projectVersions != null && !projectVersions.isEmpty())
        {
            queryString.append("(");
            for (ProjectVersion projectVersion : projectVersions)
            {
                queryString.append(" ( ");
                getArtifactAndVersionFilter(queryString, projectVersion.getGroupId(), projectVersion.getArtifactId(), projectVersion.getVersionId());
                queryString.append(") |");
            }
            queryString.setLength(queryString.length() - 1); // Removes the "|" after the last iteration
            queryString.append(") ");
        }

        return handleRegexSearchForStoredEntities(new Query(queryString.toString()), search, limit, summary);
    }

    @Override
    public List<StoredEntity> findLatestEntitiesByClassifier(String classifier, String search, Integer limit, boolean summary, boolean versioned)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versioned);
        appendQueryTagEqualCondition(query, VERSION_ID_TAG, MASTER_SNAPSHOT);

        return handleRegexSearchForStoredEntities(new Query(query.toString()), search, limit, summary);
    }

    private List<StoredEntity> handleRegexSearchForStoredEntities(Query query, String search, int limit, boolean summary)
    {
        boolean isSearchFilterProvided = false;
        if (search != null)
        {
            isSearchFilterProvided = true;
        }

        boolean isLimitProvided = false;
        if (limit > 0)
        {
            isLimitProvided = true;
            if (!isSearchFilterProvided) // IF regex matching is required THEN the results will be limited manually
            {
                query.limit(0, limit);
            }
        }

        List<StoredEntity> storedEntities = transform(summary, find(COLLECTION, query));
        if (isSearchFilterProvided && storedEntities != null)
        {
            int counter = 0;

            Iterator iterator = storedEntities.iterator();
            while (iterator.hasNext())
            {
                StoredEntityOverview storedEntityOverview = (StoredEntityOverview) iterator.next();

                if (isLimitProvided && counter == limit)
                {
                    iterator.remove();
                    continue;
                }

                if (storedEntityOverview != null)
                {
                    Pattern pattern = Pattern.compile(Pattern.quote(search), Pattern.CASE_INSENSITIVE);
                    if (!(pattern.matcher(storedEntityOverview.getPath()).find()))
                    {
                        iterator.remove();
                    } else
                    {
                        counter++;
                    }
                }
            }
        }

        return storedEntities;
    }

    @Override
    public List<StoredEntity> findReleasedEntitiesByClassifier(String classifier, boolean summary, boolean versionedEntities)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versionedEntities);
        appendQueryTagNotEqualCondition(query, VERSION_ID_TAG, MASTER_SNAPSHOT);

        return transform(summary, find(COLLECTION, new Query(query.toString())));
    }

    @Override
    public List<StoredEntity> findLatestEntitiesByClassifier(String classifier, boolean summary, boolean versioned)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(query, VERSION_ID_TAG, MASTER_SNAPSHOT);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versioned);

        return transform(summary, find(COLLECTION, new Query(query.toString())));
    }

    public List<StoredEntity> findEntitiesByClassifier(String classifier, boolean summary, boolean versioned)
    {
        StringBuffer query = new StringBuffer();

        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versioned);

        return transform(summary, find(COLLECTION, new Query(query.toString())));
    }

    @Override
    public List<StoredEntity> findEntitiesByClassifier(String groupId, String artifactId, String versionId, String classifier, boolean summary, boolean versionedEntities)
    {
        return findByClassifier(groupId, artifactId, versionId, classifier, summary, versionedEntities);
    }

    private List<StoredEntity> findByClassifier(String groupId, String artifactId, String versionId, String classifier, boolean summary, boolean versionedEntities)
    {
        StringBuffer query = new StringBuffer();

        getArtifactAndVersionFilter(query, groupId, artifactId, versionId);
        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);
        appendQueryTagEqualCondition(query, VERSIONED_ENTITY_TAG, versionedEntities);

        return transform(summary, find(COLLECTION, new Query(query.toString())));
    }

    protected List<StoredEntity> getEntitiesByClassifier(String groupId, String artifactId, String versionId, String classifier, boolean versionedEntities)
    {
        StringBuffer query = getArtifactWithVersionsFilter(new StringBuffer(), groupId, artifactId, versionId, versionedEntities);
        appendQueryTagEqualCondition(query, ENTITY_CLASSIFIER_PATH_TAG, classifier);

        return findAndConvert(COLLECTION, new Query(query.toString()));
    }

    @Override
    public List<Entity> getEntities(String groupId, String artifactId, String version, boolean versioned)
    {
        return getAllEntities(groupId, artifactId, version, versioned);
    }

    @Override
    public StoreOperationResult delete(String groupId, String artifactId, String versionId, boolean versioned)
    {
        StringBuffer query = getArtifactWithVersionsFilter(new StringBuffer(), groupId, artifactId, versionId, versioned);
        long deletedKeys = deleteByQuery(COLLECTION, new Query(query.toString()));
        return new StoreOperationResult(0, 0, deletedKeys, Collections.emptyList());
    }

    @Override
    public StoreOperationResult deleteAll(String groupId, String artifactId)
    {
        long deletedKeys = deleteByQuery(COLLECTION, new Query(getArtifactFilter(new StringBuffer(), groupId, artifactId).toString()));
        return new StoreOperationResult(0, 0, deletedKeys, Collections.emptyList());
    }

    public long getVersionEntityCount()
    {
        return count(COLLECTION, new Query(appendQueryTagNotEqualCondition(new StringBuffer(), VERSION_ID, MASTER_SNAPSHOT).toString()));
    }

    @Override
    public long getVersionEntityCount(String groupId, String artifactId, String versionId)
    {
        return count(COLLECTION, new Query(getArtifactAndVersionFilter(new StringBuffer(), groupId, artifactId, versionId).toString()));
    }

    @Override
    public List<StoredEntity> getAllStoredEntities() {
        return findAll(COLLECTION);
    }

    @Override
    public long getEntityCount(String groupId, String artifactId)
    {
        StringBuffer query = getArtifactFilter(new StringBuffer(), groupId, artifactId);
        return count(COLLECTION, new Query(query.toString()));
    }

    @Override
    public List<Pair<String, String>> getStoredEntitiesCoordinates()
    {
        AggregationBuilder aggregation = new AggregationBuilder(REDIS_QUERY_WILDCARD)
                .apply(REDIS_APPLY_CONCATENATION, "coordinate")
                .groupBy("@coordinate", Reducers.count());

        AggregationResult resultSet = redisClient.ftAggregate(COLLECTION + REDIS_QUERY_INDEX_SUFFIX, aggregation);

        List<Pair<String, String>> storedEntitiesCoordinates = new ArrayList<>();

        List<Map<String, Object>> applyAggregateResults = resultSet.getResults();
        if (applyAggregateResults != null && !applyAggregateResults.isEmpty())
        {
            for (Map<String, Object> result : applyAggregateResults)
            {
                if (result != null && result.containsKey("coordinate"))
                {
                    StringTokenizer tokenizer = new StringTokenizer(String.valueOf(result.get("coordinate")), ":");
                    storedEntitiesCoordinates.add(Tuples.pair(tokenizer.nextToken(), tokenizer.nextToken()));
                }
            }
        }
        return storedEntitiesCoordinates;
    }
}
