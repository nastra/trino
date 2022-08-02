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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.TestIcebergParquetConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.DelegatingRESTSessionCatalog;
import org.assertj.core.util.Files;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoRESTCatalogConnectorTest
        extends TestIcebergParquetConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_COMMENT_ON_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File warehouseLocation = Files.newTemporaryFolder();
        warehouseLocation.deleteOnExit();

        Catalog backend = RESTCatalogTestUtils.backendCatalog(warehouseLocation);

        DelegatingRESTSessionCatalog delegatingCatalog = DelegatingRESTSessionCatalog
                .builder().delegate(backend).build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", "PARQUET")
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest.uri", testServer.getBaseUrl().toString())
                                .buildOrThrow())
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Override
    public void testAddColumnConcurrently()
    {
        throw new SkipException("Concurrent commit not supported");
    }

    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        throw new SkipException("Backing catalog creates schema automatically.");
    }

    @Override
    public void testCreateTableSchemaNotFound()
    {
        throw new SkipException("Backing catalog creates schema automatically.");
    }

    @Override
    public void testCreateTableLike()
    {
        // FIXME: Bad constraint on table location check
        // super.testCreateTableLike();
        throw new SkipException("Backing catalog supports writing to existing locations");
    }

    @Override
    public void testShowCreateSchema()
    {
        // Overridden due to REST Catalog not supporting namespace principal
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "WITH \\(\n" +
                        "\\s+location = '.*/iceberg_data/tpch'\n" +
                        "\\)");
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("\\QCREATE TABLE iceberg.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + PARQUET.name() + "',\n" +
                        "   format_version = 2,\n" +
                        "   location = '" + tempDir + "/iceberg_data/tpch/orders'\n" +
                        ")");
    }
}
