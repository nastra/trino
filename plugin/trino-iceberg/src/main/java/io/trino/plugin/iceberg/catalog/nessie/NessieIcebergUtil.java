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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static java.lang.String.format;

final class NessieIcebergUtil
{
    private NessieIcebergUtil()
    {
    }

    static ContentKey toKey(SchemaTableName tableName)
    {
        return ContentKey.of(org.projectnessie.model.Namespace.parse(tableName.getSchemaName()),
                tableName.getTableName());
    }

    @Nullable
    static IcebergTable loadTable(NessieApiV1 nessieApi, UpdateableReference reference,
            SchemaTableName schemaTableName)
    {
        try {
            ContentKey key = NessieIcebergUtil.toKey(schemaTableName);
            Content table = nessieApi.getContent().key(key).reference(reference.getReference())
                    .get().get(key);
            return table != null ? table.unwrap(IcebergTable.class).orElse(null) : null;
        }
        catch (NessieNotFoundException e) {
            return null;
        }
    }

    static Stream<SchemaTableName> tableStream(NessieApiV1 nessieApi, UpdateableReference reference,
            Optional<String> namespace)
    {
        try {
            return nessieApi.getEntries()
                    .reference(reference.getReference())
                    .get()
                    .getEntries()
                    .stream()
                    .filter(NessieIcebergUtil.namespacePredicate(namespace))
                    .filter(e -> Type.ICEBERG_TABLE == e.getType())
                    .map(e -> new SchemaTableName(e.getName().getNamespace().name(),
                            e.getName().getName()));
        }
        catch (NessieNotFoundException ex) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, ex);
        }
    }

    static ImmutableCommitMeta buildCommitMeta(ConnectorSession session, String commitMsg)
    {
        return CommitMeta.builder()
                .message(commitMsg)
                .author(session.getUser())
                .build();
    }

    static Predicate<EntriesResponse.Entry> namespacePredicate(Optional<String> namespace)
    {
        return namespace.<Predicate<EntriesResponse.Entry>>map(
                ns -> entry -> org.projectnessie.model.Namespace.parse(ns)
                        .equals(entry.getName().getNamespace())).orElseGet(() -> e -> true);
    }

    static UpdateableReference loadReference(NessieApiV1 nessieApi, String requestedRef,
            String hash)
    {
        try {
            Reference ref = requestedRef == null ? nessieApi.getDefaultBranch()
                    : nessieApi.getReference().refName(requestedRef).get();
            if (hash != null) {
                if (ref instanceof Branch) {
                    ref = Branch.of(ref.getName(), hash);
                }
                else {
                    ref = Tag.of(ref.getName(), hash);
                }
            }
            return new UpdateableReference(ref, hash != null);
        }
        catch (NessieNotFoundException ex) {
            if (requestedRef != null) {
                throw new IllegalArgumentException(format(
                        "Nessie ref '%s' does not exist. This ref must exist before creating a NessieCatalog.",
                        requestedRef), ex);
            }

            throw new IllegalArgumentException(
                    "Nessie does not have an existing default branch." +
                            "Either configure an alternative ref via 'iceberg.nessie.ref' or create the default branch on the server.",
                    ex);
        }
    }

    static void refreshReference(NessieApiV1 nessieApi, UpdateableReference reference)
    {
        try {
            reference.refresh(nessieApi);
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR,
                    format("Failed to refresh as Reference '%s' is no longer valid.",
                            reference.getName()), e);
        }
    }
}
