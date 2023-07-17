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

package org.finos.legend.depot.store.redis.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.depot.domain.HasIdentifier;
import org.finos.legend.depot.store.StoreException;
import org.slf4j.Logger;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.JsonSetParams;
import redis.clients.jedis.json.Path;
import redis.clients.jedis.search.*;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

import static org.finos.legend.depot.domain.DatesHandler.toTime;


public abstract class BaseRedis<T extends HasIdentifier>
{
    protected static final String REDIS_JSON_ROOT = "$";
    protected static final String REDIS_QUERY_FIELD_MOD_PREFIX = "@";
    protected static final String REDIS_QUERY_WILDCARD = "*";
    protected static final String REDIS_KEY_DELIMITER = ":";
    public static final String REDIS_QUERY_INDEX_SUFFIX = REDIS_KEY_DELIMITER + "index";
    private static final Pattern REDIS_QUERY_SPECIAL_CHARACTERS_PATTERN = Pattern.compile("[-:.]");

    protected static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(BaseRedis.class);

    public static final String ID = "id";
    public static final String ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + ID;
    public static final String ARTIFACT_ID = "artifactId";
    public static final String ARTIFACT_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + ARTIFACT_ID;
    public static final String GROUP_ID = "groupId";
    public static final String GROUP_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + GROUP_ID;
    public static final String VERSION_ID = "versionId";
    public static final String VERSION_ID_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + VERSION_ID;
    public static final String CREATED = "created";
    public static final String CREATED_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + CREATED;
    public static final String UPDATED = "updated";
    public static final String UPDATED_TAG = REDIS_QUERY_FIELD_MOD_PREFIX + UPDATED;

    protected final UnifiedJedis redisClient;
    private final JsonSetParams jsonSetOnlyIfNotExistParam = JsonSetParams.jsonSetParams().nx();

    protected final Class<T> documentClass;
    protected final ObjectMapper objectMapper;

    public BaseRedis(UnifiedJedis redisClient, Class<T> documentClass)
    {
        this.redisClient = redisClient;
        this.documentClass = documentClass;
        this.objectMapper = new ObjectMapper();

    }

    public BaseRedis(UnifiedJedis redisClient, Class<T> documentClass, ObjectMapper objectMapper)
    {
        this.redisClient = redisClient;
        this.documentClass = documentClass;
        this.objectMapper = objectMapper;
    }

    protected abstract String getKey(T data);

    protected abstract Query getKeyFilter(T data);

    protected abstract void validateNewData(T data);

    protected static String buildIndex(UnifiedJedis redisClient, String collectionName, Schema schema)
    {
        String indexName = collectionName + REDIS_QUERY_INDEX_SUFFIX;
        try
        {
            redisClient.ftInfo(indexName);
            return indexName; // if index does not exist this cannot be reached
        } catch (Exception e) {}

        // For SORTABLE fields, the default ordering is ASC if not specified otherwise
        // Uniqueness will need to be managed at the INSERT level using the NX option since Redis does not support unique indexes
        IndexDefinition def = new IndexDefinition(IndexDefinition.Type.JSON).setPrefixes(new String[]{ collectionName + ":" });
        redisClient.ftCreate(indexName, IndexOptions.defaultOptions().setDefinition(def), schema);

        return indexName;
    }

    protected StringBuffer appendQueryTagEqualCondition(StringBuffer query, String fieldModifier, Object fieldValue)
    {
        if (fieldValue instanceof String)
        {
            fieldValue = handleSpecialCharacters(String.valueOf(fieldValue));
        }
        return query.append(fieldModifier) .append(":{ ") .append(fieldValue) .append(" } ");
    }

    protected StringBuffer appendQueryTagLessThanCondition(StringBuffer query, String fieldModifier, Object fieldValue)
    {
        if (fieldValue instanceof String)
        {
            fieldValue = handleSpecialCharacters(String.valueOf(fieldValue));
        }
        return query.append(fieldModifier) .append(":[-inf (") .append(fieldValue) .append("] ");
    }

    protected StringBuffer appendQueryTagLessThanOrEqualToCondition(StringBuffer query, String fieldModifier, Object fieldValue)
    {
        if (fieldValue instanceof String)
        {
            fieldValue = handleSpecialCharacters(String.valueOf(fieldValue));
        }
        return query.append(fieldModifier) .append(":[-inf ") .append(fieldValue) .append("] ");
    }

    protected StringBuffer appendQueryTagGreaterThanOrEqualToCondition(StringBuffer query, String fieldModifier, Object fieldValue)
    {
        if (fieldValue instanceof String)
        {
            fieldValue = handleSpecialCharacters(String.valueOf(fieldValue));
        }
        return query.append(fieldModifier) .append(":[") .append(fieldValue) .append(" inf] ");
    }

    protected StringBuffer appendQueryTagNotEqualCondition(StringBuffer query, String fieldModifier, Object fieldValue)
    {
        if (fieldValue instanceof String)
        {
            fieldValue = handleSpecialCharacters(String.valueOf(fieldValue));
        }
        return query.append("-") .append(REDIS_QUERY_FIELD_MOD_PREFIX) .append(fieldModifier) .append(":{ ") .append(fieldValue) .append(" } ");
    }

    protected StringBuffer appendQueryTagPrefixCondition(StringBuffer query, String fieldModifier, String prefix)
    {
        if (prefix instanceof String)
        {
            prefix = handleSpecialCharacters(prefix);
        }
        return query.append(fieldModifier) .append(":{ ") .append(prefix) .append("*} ");
    }

    protected long count(String collectionName, Query query) {
        query.limit(0,0);

        SearchResult countResult = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        return countResult != null ? countResult.getTotalResults() : 0;
    }

    protected T createOrUpdate(String collectionName, boolean uniqueIndex, boolean isIdRequired, T inputObject)
    {
        validateNewData(inputObject);

        Optional<Document> queryResult = findOne(collectionName, getKeyFilter(inputObject));
        if (queryResult.isPresent())
        {
            Document document = queryResult.get();

            Map<String, Object> updatedPropertiesMap = convertDomainToPropertiesMap(inputObject);
            handleCreateUpdateDates(updatedPropertiesMap, document);

            insert(document.getId(), false, isIdRequired, updatedPropertiesMap);
            return convertPropertiesMapToDomain(updatedPropertiesMap, documentClass);
        }
        else
        {
            insert(getKey(inputObject), uniqueIndex, isIdRequired, inputObject);
            return inputObject;
        }
    }

    public void insert(String key, boolean isIndexUnique, boolean isIdRequired, T inputObject)
    {
        validateNewData(inputObject);

        key = key == null ? getKey(inputObject) : key;

        insert(key, isIndexUnique, isIdRequired, handleCreateUpdateDates(convertDomainToPropertiesMap(inputObject)));
    }

    public void insert(String key, boolean isIndexUnique, boolean isIdRequired, Map<String, Object> propertiesMap)
    {
        String status;

        if (isIdRequired)
        {
            propertiesMap.putIfAbsent(ID, key);
        }

        if (isIndexUnique)
        {
            status = redisClient.jsonSet(key, Path.ROOT_PATH, propertiesMap, jsonSetOnlyIfNotExistParam);
        } else {
            status = redisClient.jsonSet(key, Path.ROOT_PATH, propertiesMap);
        }

        if (!"OK".equals(status))
        {
            throw new StoreException("Error inserting dataset with key: '" + key + "' - ensure the key is unique");
        }
    }

    protected long deleteByKey(String key)
    {
        if (key == null || key.isEmpty())
            return 0;

        long result = redisClient.unlink(key);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("delete key:{}", key);
        }
        return result;
    }

    protected long deleteByAggregation(String collectionName, AggregationBuilder aggregation)
    {
        AggregationResult result = redisClient.ftAggregate(collectionName + REDIS_QUERY_INDEX_SUFFIX, aggregation);
        if (result == null)
            return 0;

        long counter = 0;

        List<Map<String, Object>> documents = result.getResults();
        if (documents != null && !documents.isEmpty()) {
            for (Map<String, Object> document : documents)
            {
                counter = counter + deleteByKey(String.valueOf(document.get(ID)));
            }
        }
        return counter;
    }

    protected long deleteByQuery(String collectionName, Query query)
    {
        SearchResult result = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        if (result == null)
            return 0;

        long counter = 0;

        List<Document> documents = result.getDocuments();
        if (documents != null && !documents.isEmpty()) {
            for (Document document : documents)
            {
                counter = counter + deleteByKey(document.getId());
            }
        }
        return counter;
    }

    protected List<Document> find(String collectionName, Query query)
    {
        SearchResult searchResult = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        if (searchResult != null) {
            return searchResult.getDocuments();
        }
        return null;
    }

    protected List<T> findAndConvert(String collectionName, Query query)
    {
        SearchResult result = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        if (result != null)
        {
            return convertDocumentsToDomainList(result.getDocuments());
        }
        return null;
    }

    protected List<T> findAndConvert(String collectionName, AggregationBuilder aggregation)
    {
        AggregationResult result = redisClient.ftAggregate(collectionName + REDIS_QUERY_INDEX_SUFFIX, aggregation);
        if (result != null)
        {
            return convertPropertiesMapsToDomains(result.getResults());
        }
        return null;
    }

    protected Optional<Document> findOne(String collectionName, Query query)
    {
        SearchResult searchResult = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        if (searchResult == null) {
            return Optional.empty();
        }

        List<Document> documents = searchResult.getDocuments();
        if (documents.isEmpty()) {
            return Optional.empty();
        }
        else if (documents.size() > 1) {
            throw new IllegalStateException(String.format(" Found more than one match %s in collection %s", query.toString(), collectionName));
        }
        return Optional.of(documents.get(0));
    }

    protected Optional<T> findOneAndConvert(String collectionName, Query query)
    {
        SearchResult searchResult = redisClient.ftSearch(collectionName + REDIS_QUERY_INDEX_SUFFIX, query);
        if (searchResult == null) {
            return Optional.empty();
        }

        List<T> documents = convertDocumentsToDomainList(searchResult.getDocuments());
        if (documents.isEmpty()) {
            return Optional.empty();
        }
        else if (documents.size() > 1) {
            throw new IllegalStateException(String.format(" Found more than one match %s in collection %s", query.toString(), collectionName));
        }
        return Optional.of(documents.get(0));
    }

    public List<T> findAll(String collectionName)
    {
        return findAndConvert(collectionName, new Query(REDIS_QUERY_WILDCARD));
    }

    public List<T> findAllByPage(String collectionName, int page, int pageSize)
    {
        return findAndConvert(collectionName, new Query(REDIS_QUERY_WILDCARD)
                .limit(Math.max(page - 1, 0) * pageSize, pageSize));
    }

    protected List<T> convertDocumentsToDomainList(List<Document> documents)
    {
        List<T> result = new ArrayList<>();
        if (documents != null) {
            documents.forEach(doc -> result.add(convertDocumentToDomain(doc, documentClass)));
        }
        return result;
    }

    public <T extends HasIdentifier> Map<String, Object> convertDomainToPropertiesMap(T object)
    {
        try
        {
            return objectMapper.readValue(objectMapper.writeValueAsString(object), new TypeReference<Map<String, Object>>() {});
        }
        catch (JsonProcessingException e)
        {
            LOGGER.error("Error serializing document to json", e);
            throw new StoreException("Error serializing dataset to json");
        }
    }

    private <T> T convertDocumentToDomain(Document document, Class<T> clazz)
    {
        if (document == null || !document.hasProperty(REDIS_JSON_ROOT))
        {
            return null;
        }

        String jsonString = String.valueOf(document.get(REDIS_JSON_ROOT));
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }

        try
        {
            Map<String, Object> properties = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
            return objectMapper.convertValue(properties, clazz);
        }
        catch (Exception e)
        {
            LOGGER.error(String.format("error converting document (%s) to class %s. reason: %s", document.getId(), clazz.getSimpleName(), e.getMessage()));
            return null;
        }
    }

    private Map<String, Object> convertDocumentToPropertiesMap(Document document, Class<T> clazz)
    {
        if (document == null && document.hasProperty(REDIS_JSON_ROOT))
        {
            return null;
        }

        String jsonString = String.valueOf(document.get(REDIS_JSON_ROOT));
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }

        try
        {
            return objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
        }
        catch (Exception e)
        {
            LOGGER.error(String.format("error converting document (%s) to class %s. reason: %s", document.getId(), clazz.getSimpleName(), e.getMessage()));
            return null;
        }
    }

    public <T> T convertPropertiesMapToDomain(Map<String, Object> propertiesMap, Class<T> clazz)
    {
        try
        {
            return objectMapper.convertValue(propertiesMap, clazz);
        }
        catch (Exception e)
        {
            LOGGER.error(String.format("error converting document to class %s. reason: %s", clazz.getSimpleName(), e.getMessage()));
            return null;
        }
    }

    protected List<T> convertPropertiesMapsToDomains(List<Map<String, Object>> propertiesMaps)
    {
        List<T> result = new ArrayList<>();
        if (propertiesMaps != null) {
            propertiesMaps.forEach(doc -> result.add(convertPropertiesMapToDomain(doc, documentClass)));
        }
        return result;
    }

    private Map<String, Object> handleCreateUpdateDates(Map<String, Object> propertiesMap)
    {
        propertiesMap.putIfAbsent(CREATED, toTime(LocalDateTime.now()));
        propertiesMap.put(UPDATED, toTime(LocalDateTime.now()));
        return propertiesMap;
    }

    private Map<String, Object> handleCreateUpdateDates(Map<String, Object> propertiesMap, Document document)
    {
        long createTime = document.hasProperty(CREATED) ?
                Long.parseLong(document.get(CREATED).toString()) : toTime(LocalDateTime.now());

        propertiesMap.putIfAbsent(CREATED, createTime);
        propertiesMap.put(UPDATED, toTime(LocalDateTime.now()));
        return propertiesMap;
    }

    protected StringBuffer getArtifactFilter(StringBuffer query, String groupId, String artifactId)
    {
        appendQueryTagEqualCondition(query, GROUP_ID_TAG, groupId);
        appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, artifactId);
        return query;
    }

    protected StringBuffer getArtifactAndVersionFilter(StringBuffer query, String groupId, String artifactId, String versionId) {
        appendQueryTagEqualCondition(query, GROUP_ID_TAG, groupId);
        appendQueryTagEqualCondition(query, ARTIFACT_ID_TAG, artifactId);
        appendQueryTagEqualCondition(query, VERSION_ID_TAG, versionId);
        return query;
    }

    private String handleSpecialCharacters(String input) {
        return REDIS_QUERY_SPECIAL_CHARACTERS_PATTERN.matcher(input).replaceAll("\\\\$0");
    }

}