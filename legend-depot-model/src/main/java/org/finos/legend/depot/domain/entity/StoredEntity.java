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

package org.finos.legend.depot.domain.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.finos.legend.depot.domain.VersionedData;
import org.finos.legend.depot.domain.HasIdentifier;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StoredEntity extends VersionedData implements HasIdentifier
{
    @JsonProperty
    private EntityDefinition entity;

    @JsonProperty
    private boolean versionedEntity = false;

    @JsonCreator
    public StoredEntity(@JsonProperty(value = "groupId") String groupId,
                        @JsonProperty(value = "artifactId") String artifactId,
                        @JsonProperty(value = "versionId") String versionId,
                        @JsonProperty(value = "entity") EntityDefinition entity)
    {
        super(groupId, artifactId, versionId);
        this.entity = entity;
    }

    StoredEntity(String groupId, String artifactId, String versionId)
    {
        super(groupId, artifactId, versionId);
    }

    public void setVersionedEntity(boolean versionedEntity)
    {
        this.versionedEntity = versionedEntity;
    }
    
    public boolean isVersionedEntity()
    {
        return versionedEntity;
    }

    @Override
    public String getId()
    {
        return "";
    }

    public EntityDefinition getEntity()
    {
        return entity;
    }

    @Override
    public boolean equals(Object obj)
    {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
