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

import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoIcebergRESTCatalogFactory
        implements TrinoCatalogFactory
{
    private static final Logger log = Logger.get(TrinoIcebergRESTCatalogFactory.class);
    private final CatalogName catalogName;
    private final IcebergConfig config;
    private final String trinoVersion;

    private volatile RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRESTCatalogFactory(
            CatalogName catalogName,
            IcebergConfig icebergConfig,
            NodeVersion nodeVersion)
    {
        this.catalogName = catalogName;
        this.config = icebergConfig;
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    Map<String, String> properties = new HashMap<>();
                    config.getBaseUri().ifPresent(v -> properties.put(CatalogProperties.URI, v));
                    config.getCredential().ifPresent(v -> properties.put(OAuth2Properties.CREDENTIAL, v));
                    config.getToken().ifPresent(v -> properties.put(OAuth2Properties.TOKEN, v));
                    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
                    properties.put("version", trinoVersion);

                    icebergCatalog = new RESTSessionCatalog();
                    try {
                        icebergCatalog.initialize(catalogName.toString(), properties);
                    }
                    catch (Exception e) {
                        icebergCatalog = null;
                        log.error("REST session catalog initialization failed. Reason: %s", e.getMessage());
                        throw e;
                    }
                }
            }
        }

        return new TrinoRESTCatalog(icebergCatalog, catalogName, trinoVersion);
    }
}
