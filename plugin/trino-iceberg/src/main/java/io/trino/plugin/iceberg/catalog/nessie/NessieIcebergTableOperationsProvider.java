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

import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import org.projectnessie.client.api.NessieApiV1;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class NessieIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final FileIoProvider fileIoProvider;
    private final NessieApiV1 nessieApi;
    private final UpdateableReference reference;

    @Inject
    public NessieIcebergTableOperationsProvider(FileIoProvider fileIoProvider, NessieApiV1 nessieApi, UpdateableReference reference)
    {
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        this.nessieApi = requireNonNull(nessieApi, "nessieApi is null");
        this.reference = reference;
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new NessieIcebergTableOperations(
                nessieApi,
                reference,
                fileIoProvider.createFileIo(new HdfsContext(session), session.getQueryId()),
                session,
                database,
                table,
                owner,
                location);
    }
}
