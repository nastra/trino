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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.NessieContainer;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseIcebergNessieConnectorTest
        extends BaseIcebergConnectorTest
{
    private NessieContainer nessieContainer;
    private Path tempDir;

    public BaseIcebergNessieConnectorTest(IcebergFileFormat format)
    {
        super(format);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
        tempDir = Files.createTempDirectory("test_trino_nessie_" + format.name());
        super.init();
    }

    @BeforeClass(dependsOnMethods = "init")
    @Override
    public void addMockCatalog()
    {
        super.addMockCatalog();
    }

    @AfterClass
    public void teardown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
        if (nessieContainer != null) {
            nessieContainer.close();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie.default-warehouse-dir", tempDir.resolve("iceberg_data").toString()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder().addAll(REQUIRED_TPCH_TABLES).add(LINE_ITEM).build())
                                .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW, SUPPORTS_COMMENT_ON_VIEW, SUPPORTS_CREATE_MATERIALIZED_VIEW, SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(500);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Key too long, max allowed length: 500");
    }

    @Override
    protected int maxTableRenameLength()
    {
        // 500 (actual limit) - 4 (namespace length=tpch)
        return 500 - 4;
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Failed to create file.*|.*Key too long, max allowed length: 500");
    }

    @Override
    protected void verifyInvalidTargetTableDoesNotExist(String invalidTargetTableName)
    {
        assertThatThrownBy(() -> getQueryRunner().tableExists(getSession(), invalidTargetTableName))
                .hasMessageContaining("Key too long, max allowed length: 500");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testFederatedMaterializedView()
    {
        assertThatThrownBy(super::testFederatedMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch");
    }

    @Override
    public void testMaterializedViewSnapshotSummariesHaveTrinoQueryId()
    {
        assertThatThrownBy(super::testMaterializedViewSnapshotSummariesHaveTrinoQueryId)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testUpdateRowConcurrently()
    {
        throw new SkipException("skipped for now due to flakiness");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        throw new SkipException("skipped for now due to flakiness");
    }

    @Override
    public void testAddColumnConcurrently()
    {
        throw new SkipException("skipped for now due to flakiness");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .getCause()
                .hasMessageContaining("Cannot commit: ref hash is out of date. Update the ref 'main' and try again");
    }

    @Override
    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        assertThat(e)
                .getCause()
                .hasMessageContaining("Cannot commit: ref hash is out of date. Update the ref 'main' and try again");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e)
                .getCause()
                .hasMessageContaining("Cannot commit: ref hash is out of date. Update the ref 'main' and try again");
    }
}
