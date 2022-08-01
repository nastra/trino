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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.assertj.core.util.Files;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RESTCatalogTestUtils
{
    private RESTCatalogTestUtils()
    {
    }

    public static Catalog backendCatalog(File warehouseLocation)
    {
        Map<String, String> properties = new HashMap<>();
        //properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
        properties.put(CatalogProperties.URI, "jdbc:sqlite:file:" + Files.newTemporaryFile().getAbsolutePath() + "_mode=memory");

        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.toPath().resolve("iceberg_data").toFile().getAbsolutePath());

        ConnectorSession connectorSession = TestingConnectorSession.builder().build();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(connectorSession);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(hdfsEnvironment.getConfiguration(context, new Path(warehouseLocation.getAbsolutePath())));
        catalog.initialize("backend_jdbc", properties);

        return catalog;
    }
}
