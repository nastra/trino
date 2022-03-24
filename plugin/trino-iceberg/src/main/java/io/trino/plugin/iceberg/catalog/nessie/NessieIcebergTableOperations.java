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

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Operation;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.catalog.nessie.NessieIcebergUtil.buildCommitMeta;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NessieIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final NessieApiV1 nessieApi;
    private final UpdateableReference reference;

    protected NessieIcebergTableOperations(
            NessieApiV1 nessieApi,
            UpdateableReference reference,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.nessieApi = requireNonNull(nessieApi, "nessieApi is null");
        this.reference = reference;
    }

    @Override
    public TableMetadata refresh()
    {
        NessieIcebergUtil.refreshReference(nessieApi, reference);
        return super.refresh();
    }

    @Override
    protected String getRefreshedLocation()
    {
        IcebergTable table = NessieIcebergUtil.loadTable(nessieApi, reference,
                new SchemaTableName(database, tableName));

        if (null == table) {
            throw new TableNotFoundException(getSchemaTableName());
        }

        return table.getMetadataLocation();
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        reference.checkMutable();
        verify(version == -1, "commitNewTable called on a table which already exists");
        doCommit(metadata, writeNewMetadata(metadata, 0));
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        reference.checkMutable();
        verify(version >= 0, "commitToExistingTable called on a new table");
        doCommit(metadata, writeNewMetadata(metadata, version + 1));
        shouldRefresh = true;
    }

    private void doCommit(TableMetadata metadata, String metadataLocation)
    {
        SchemaTableName tableName = new SchemaTableName(database, this.tableName);
        try {
            ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder();
            Snapshot snapshot = metadata.currentSnapshot();
            long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;
            IcebergTable newTable = newTableBuilder
                    .snapshotId(snapshotId)
                    .schemaId(metadata.currentSchemaId())
                    .specId(metadata.defaultSpecId())
                    .sortOrderId(metadata.defaultSortOrderId())
                    .metadataLocation(metadataLocation)
                    .build();

            Branch branch = nessieApi.commitMultipleOperations()
                    .operation(Operation.Put.of(NessieIcebergUtil.toKey(tableName), newTable))
                    .commitMeta(
                            buildCommitMeta(session, format("Trino Iceberg add table %s",
                                    tableName)))
                    .branch(reference.getAsBranch())
                    .commit();
            reference.updateReference(branch);
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR,
                    format("Cannot commit: Reference %s no longer exists",
                            reference.getName()), e);
        }
        catch (NessieConflictException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR,
                    format("Cannot commit: Reference hash is out of date. " +
                            "Update the reference %s and try again", reference.getName()), e);
        }
    }
}
