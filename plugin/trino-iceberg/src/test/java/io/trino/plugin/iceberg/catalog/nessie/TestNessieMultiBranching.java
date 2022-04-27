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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.NessieContainer;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNessieMultiBranching
        extends AbstractTestQueryFramework
{
    private NessieContainer nessieContainer;
    private Path tempDir;
    private NessieApiV1 nessieApiV1;

    @BeforeClass
    public void init()
            throws Exception
    {
        nessieContainer = closeAfterClass(NessieContainer.builder().build());
        nessieContainer.start();
        tempDir = Files.createTempDirectory("test_nessie_multi_branching");
        nessieApiV1 = closeAfterClass(HttpClientBuilder.builder().withUri(nessieContainer.getRestApiUri()).build(NessieApiV1.class));
        super.init();
    }

    @AfterClass
    public void teardown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", PARQUET.name(),
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie.default-warehouse-dir", tempDir.toString()))
                .build();
    }

    @Test
    public void testWithUnknownBranch()
    {
        assertQueryFails(sessionOnRef("unknownRef"), "CREATE SCHEMA nessie_namespace",
                "Nessie ref 'unknownRef' does not exist. This ref must exist before creating a NessieCatalog.");
    }

    @Test
    public void testNamespaceVisibility()
            throws NessieConflictException, NessieNotFoundException
    {
        Reference one = createBranch("branchOne");
        Reference two = createBranch("branchTwo");
        Session sessionOne = sessionOnRef(one.getName());
        Session sessionTwo = sessionOnRef(two.getName());
        assertQuerySucceeds(sessionOne, "CREATE SCHEMA namespace_one");
        assertQuerySucceeds(sessionOne, "SHOW CREATE SCHEMA namespace_one");
        assertQuerySucceeds(sessionTwo, "CREATE SCHEMA namespace_two");
        assertQuerySucceeds(sessionTwo, "SHOW CREATE SCHEMA namespace_two");

        // namespace_two shouldn't be visible on branchOne
        assertQueryFails(sessionOne, "SHOW CREATE SCHEMA namespace_two", ".*Schema 'iceberg.namespace_two' does not exist");
        // namespace_one shouldn't be visible on branchTwo
        assertQueryFails(sessionTwo, "SHOW CREATE SCHEMA namespace_one", ".*Schema 'iceberg.namespace_one' does not exist");
    }

    @Test
    public void testNamespacePropertiesVisibility()
            throws NessieConflictException, NessieNotFoundException
    {
        Reference one = createBranch("branchOneWithProperties");
        Reference two = createBranch("branchTwoWithProperties");
        Session sessionOne = sessionOnRef(one.getName());
        Session sessionTwo = sessionOnRef(two.getName());
        assertQuerySucceeds(sessionOne, "CREATE SCHEMA namespace_one WITH (location = 'x')");
        assertQuerySucceeds(sessionTwo, "CREATE SCHEMA namespace_one WITH (location = 'y')");

        assertThat((String) computeScalar(sessionOne, "SHOW CREATE SCHEMA namespace_one"))
                .contains("location = 'x'");
        assertThat((String) computeScalar(sessionTwo, "SHOW CREATE SCHEMA namespace_one"))
                .contains("location = 'y'");

        assertQuerySucceeds(sessionOne, "DROP SCHEMA namespace_one");
        assertQueryFails(sessionOne, "SHOW CREATE SCHEMA namespace_one", ".*Schema 'iceberg.namespace_one' does not exist");
        // should still be visible on second branch
        assertQuerySucceeds(sessionTwo, "SHOW CREATE SCHEMA namespace_one");
    }

    @Test
    public void testTableDataVisibility()
            throws NessieConflictException, NessieNotFoundException
    {
        assertQuerySucceeds("CREATE SCHEMA namespace_one");
        assertQuerySucceeds("CREATE TABLE namespace_one.tbl (a int)");
        assertQuerySucceeds("INSERT INTO namespace_one.tbl (a) VALUES (1)");
        assertQuerySucceeds("INSERT INTO namespace_one.tbl (a) VALUES (2)");

        Reference one = createBranch("branchOneWithTable");
        Reference two = createBranch("branchTwoWithTable");
        Session sessionOne = sessionOnRef(one.getName());
        Session sessionTwo = sessionOnRef(two.getName());

        assertQuerySucceeds(sessionOne, "INSERT INTO namespace_one.tbl (a) VALUES (3)");

        assertQuerySucceeds(sessionTwo, "INSERT INTO namespace_one.tbl (a) VALUES (5)");
        assertQuerySucceeds(sessionTwo, "INSERT INTO namespace_one.tbl (a) VALUES (6)");

        // main branch should still have 2 entries
        assertThat(computeScalar("SELECT count(*) FROM namespace_one.tbl")).isEqualTo(2L);
        MaterializedResult rows = computeActual("SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(2);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(getSession(), rows.getTypes())
                .row(1)
                .row(2)
                .build().getMaterializedRows());

        // there should be 3 entries on this branch
        assertThat(computeScalar(sessionOne, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(3L);
        rows = computeActual(sessionOne, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(3);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionOne, rows.getTypes())
                .row(1)
                .row(2)
                .row(3)
                .build().getMaterializedRows());

        // and 4 entries on this branch
        assertThat(computeScalar(sessionTwo, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(4L);
        rows = computeActual(sessionTwo, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(4);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionTwo, rows.getTypes())
                .row(1)
                .row(2)
                .row(5)
                .row(6)
                .build().getMaterializedRows());

        // retrieve the second to the last commit hash and query the table with that hash
        List<LogResponse.LogEntry> logEntries = nessieApiV1.getCommitLog().refName(two.getName()).get().getLogEntries();
        assertThat(logEntries).isNotEmpty();
        String hash = logEntries.get(1).getCommitMeta().getHash();
        Session sessionTwoAtHash = sessionOnRef(two.getName(), hash);

        // at this hash there were only 3 rows
        assertThat(computeScalar(sessionTwoAtHash, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(3L);
        rows = computeActual(sessionTwoAtHash, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(3);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionTwoAtHash, rows.getTypes())
                .row(1)
                .row(2)
                .row(5)
                .build().getMaterializedRows());
    }

    private Session sessionOnRef(String reference)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "nessie_reference_name", reference)
                .build();
    }

    private Session sessionOnRef(String reference, String hash)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "nessie_reference_name", reference)
                .setCatalogSessionProperty("iceberg", "nessie_reference_hash", hash)
                .build();
    }

    private Reference createBranch(String branchName)
            throws NessieConflictException, NessieNotFoundException
    {
        Reference main = nessieApiV1.getReference().refName("main").get();
        return nessieApiV1.createReference().sourceRefName(main.getName()).reference(Branch.of(branchName, main.getHash())).create();
    }
}
