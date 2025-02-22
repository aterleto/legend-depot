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

package org.finos.legend.depot.domain.version;

import org.finos.legend.sdlc.domain.model.version.VersionId;

public class VersionValidator
{
    public static String BRANCH_SNAPSHOT(String branchName)
    {
        return branchName + SNAPSHOT;
    }

    private static final String SNAPSHOT = "-SNAPSHOT";
    public static final String VALID_VERSION_ID_TXT = "a valid version string: x.y.z, master-SNAPSHOT or alias: latest = last released version, head = latest unreleased revision";

    private VersionValidator()
    {
    }

    public static boolean isValid(String versionId)
    {
        return versionId != null && !versionId.isEmpty() && (isSnapshotVersion(versionId) || isValidReleaseVersion(versionId));
    }

    public static boolean isValidReleaseVersion(String versionId)
    {
        try
        {
            VersionId.parseVersionId(versionId);
            return true;
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }
    }

    public static boolean isSnapshotVersion(String versionId)
    {
        return versionId.endsWith(SNAPSHOT);
    }
}
