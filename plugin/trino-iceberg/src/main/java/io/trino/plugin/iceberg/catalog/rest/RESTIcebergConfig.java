/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.configuration.Config;

import java.util.Optional;

public class RESTIcebergConfig
{
    private String credential;
    private String token;
    private String restUri;

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("iceberg.rest.token")
    public RESTIcebergConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public Optional<String> getCredential()
    {
        return Optional.ofNullable(credential);
    }

    @Config("iceberg.rest.credential")
    public RESTIcebergConfig setCredential(String credential)
    {
        this.credential = credential;
        return this;
    }

    public String getBaseUri()
    {
        return this.restUri;
    }

    @Config("iceberg.rest.uri")
    public RESTIcebergConfig setBaseUri(String uri)
    {
        this.restUri = uri;
        return this;
    }
}
