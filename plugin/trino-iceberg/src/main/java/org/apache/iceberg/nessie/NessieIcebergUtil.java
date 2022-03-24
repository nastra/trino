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
package org.apache.iceberg.nessie;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static java.lang.String.format;

// once Iceberg 1.1.0 ships and UpdateableReference is public, we can move this class to io.trino.plugin.iceberg.catalog.nessie
public final class NessieIcebergUtil
{
    private NessieIcebergUtil() {}

    public static ContentKey toKey(SchemaTableName tableName)
    {
        return ContentKey.of(Namespace.parse(tableName.getSchemaName()), tableName.getTableName());
    }

    public static ImmutableCommitMeta buildCommitMeta(String author, String commitMsg)
    {
        return CommitMeta.builder()
                .message(commitMsg)
                .author(author)
                .build();
    }

    public static TableIdentifier toIdentifier(SchemaTableName schemaTableName)
    {
        return TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    public static void commitTable(NessieIcebergClient nessieClient, TableMetadata metadata, SchemaTableName tableName, String metadataLocation, String user)
    {
        nessieClient.getRef().checkMutable();
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

            Branch branch = nessieClient.getApi().commitMultipleOperations()
                    .operation(Operation.Put.of(NessieIcebergUtil.toKey(tableName), newTable))
                    .commitMeta(buildCommitMeta(user, format("Trino Iceberg add table %s", tableName)))
                    .branch(nessieClient.getRef().getAsBranch())
                    .commit();
            nessieClient.getRef().updateReference(branch);
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit: ref '%s' no longer exists", nessieClient.getRef().getName()), e);
        }
        catch (NessieConflictException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Cannot commit: ref hash is out of date. Update the ref '%s' and try again", nessieClient.getRef().getName());
        }
    }
}
