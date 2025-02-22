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

package org.finos.legend.depot.core.http.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.finos.legend.depot.domain.DatesHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

@Api("Info")
@Path("")
public class InfoResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InfoResource.class);
    private static final String PLATFORM_VERSION_FILE = "version.json";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long MEGABYTE = 1024L * 1024L;


    @Inject
    public InfoResource()
    {
        //no init required
    }

    private static <T> T tryGetValue(InfoResource.SupplierWithException<T> supplier)
    {
        try
        {
            return supplier.get();
        }
        catch (Exception var2)
        {
            LOGGER.warn("Error getting info property", var2);
            return null;
        }
    }

    private static String getLocalHostName() throws UnknownHostException
    {
        return InetAddress.getLocalHost().getHostName();
    }

    private static InfoResource.PlatformVersionInfo getPlatformVersionInfo() throws IOException
    {
        URL url = InfoResource.class.getClassLoader().getResource(PLATFORM_VERSION_FILE);
        return url == null ? null : JSON.readValue(url, PlatformVersionInfo.class);
    }

    @GET
    @Path("/info")
    @Produces({"application/json"})
    @ApiOperation("Provides server information")
    public InfoResource.ServerInfo getServerInfo()
    {
        String hostName = tryGetValue(InfoResource::getLocalHostName);
        InfoResource.PlatformVersionInfo platformVersionInfo = tryGetValue(InfoResource::getPlatformVersionInfo);
        return new InfoResource.ServerInfo(hostName, platformVersionInfo);
    }

    private interface SupplierWithException<T>
    {
        T get() throws Exception;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PlatformVersionInfo
    {

        @JsonProperty("git.build.version")
        public String version;
        @JsonProperty("git.build.time")
        public String buildTime;
        @JsonProperty("git.commit.id")
        public String gitRevision;

        private PlatformVersionInfo()
        {
        }
    }

    public static class ServerPlatformInfo
    {
        private final String version;
        private final String buildTime;
        private final String buildRevision;

        private ServerPlatformInfo(String version, String buildTime, String buildRevision)
        {
            this.version = version;
            this.buildTime = buildTime;
            this.buildRevision = buildRevision;
        }

        private ServerPlatformInfo()
        {
            this(null, null, null);
        }

        public String getVersion()
        {
            return this.version;
        }

        public String getBuildTime()
        {
            return this.buildTime;
        }

        public String getBuildRevision()
        {
            return this.buildRevision;
        }
    }

    public class ServerInfo
    {
        private final String hostName;
        private final long totalMemory;
        private final long maxMemory;
        private final long usedMemory;
        private final String serverTimeZone;
        private final InfoResource.ServerPlatformInfo platform;

        private ServerInfo(String hostName, InfoResource.PlatformVersionInfo platformVersionInfo)
        {
            this.hostName = hostName;
            this.platform = platformVersionInfo == null ? new InfoResource.ServerPlatformInfo() : new InfoResource.ServerPlatformInfo(platformVersionInfo.version, platformVersionInfo.buildTime, platformVersionInfo.gitRevision);
            this.totalMemory = Runtime.getRuntime().totalMemory() / MEGABYTE;
            this.maxMemory = Runtime.getRuntime().maxMemory() / MEGABYTE;
            this.usedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / MEGABYTE;
            this.serverTimeZone = DatesHandler.ZONE_ID.getId();
        }

        public String getHostName()
        {
            return this.hostName;
        }

        public long getMaxMemory()
        {
            return maxMemory;
        }

        public long getUsedMemory()
        {
            return usedMemory;
        }

        public long getTotalMemory()
        {
            return totalMemory;
        }

        public InfoResource.ServerPlatformInfo getPlatform()
        {
            return this.platform;
        }

        public String getServerTimeZone()
        {
            return serverTimeZone;
        }
    }

}