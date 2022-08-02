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
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class TrinoRESTCatalog
        implements TrinoCatalog
{
    private final RESTSessionCatalog sessionCatalog;
    private final CatalogName catalogName;
    private final String trinoVersion;

    private final ConcurrentHashMap<String, Table> tableCache = new ConcurrentHashMap<>();

    public TrinoRESTCatalog(
            RESTSessionCatalog sessionCatalog,
            CatalogName catalogName,
            String trinoVersion)
    {
        this.sessionCatalog = sessionCatalog;
        this.catalogName = catalogName;
        this.trinoVersion = trinoVersion;
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return sessionCatalog.listNamespaces(convert(session)).stream()
                .map(Namespace::toString).collect(Collectors.toList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        sessionCatalog.dropNamespace(convert(session), Namespace.of(namespace));
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return new HashMap<>(sessionCatalog.loadNamespaceMetadata(convert(session), Namespace.of(namespace)));
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(!properties.containsKey(IcebergTableProperties.LOCATION_PROPERTY), "Database cannot be created with a location set");

        sessionCatalog.createNamespace(convert(session), Namespace.of(namespace),
                properties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().toString())));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        List<String> namespaces = namespace.map(List::of).orElseGet(() -> listNamespaces(session));
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String ns : namespaces) {
            tables.addAll(
                    sessionCatalog.listTables(convert(session), Namespace.of(ns))
                            .stream()
                            .map(id -> SchemaTableName.schemaTableName(id.namespace().toString(),
                                    id.name()))
                            .collect(Collectors.toList()));
        }
        return tables.build();
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String location, Map<String, String> properties)
    {
        return sessionCatalog.buildTable(convert(session), toIdentifier(schemaTableName), schema)
                .withPartitionSpec(partitionSpec)
                .withLocation(location)
                .withProperties(properties)
                .createTransaction();
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (!sessionCatalog.dropTable(convert(session), toIdentifier(schemaTableName))) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to drop table: %s", schemaTableName));
        }
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        try {
            sessionCatalog.renameTable(convert(session), toIdentifier(from), toIdentifier(to));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return tableCache.computeIfAbsent(
                    schemaTableName.toString(),
                    key -> {
                        BaseTable baseTable = (BaseTable) sessionCatalog.loadTable(convert(session), toIdentifier(schemaTableName));
                        return new BaseTable(baseTable.operations(), IcebergUtil.quotedTableName(schemaTableName));
                    });
        }
        catch (NoSuchTableException e) {
            throw new TableNotFoundException(schemaTableName, e);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = sessionCatalog.loadTable(convert(session), toIdentifier(schemaTableName));
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return null;
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return Collections.emptyList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return new HashMap<>();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return Collections.emptyList();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating materialized views");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        loadTable(session, schemaTableName).updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @SuppressModernizer
    private SessionCatalog.SessionContext convert(ConnectorSession session)
    {
        String sessionId = format("%s-%s", session.getUser(), session.getSource().orElse("default"));
        String identity;

        if (session.getIdentity().getPrincipal().isPresent()) {
            identity = session.getIdentity().getPrincipal().get().getName();
        }
        else {
            identity = session.getUser();
        }

        Map<String, Object> extraCredentialsClaims = session.getIdentity().getExtraCredentials()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> "extra:" + e.getKey(),
                        Map.Entry::getValue));

        String subjectJwt = new DefaultJwtBuilder()
                .setSubject(identity)
                .setIssuer(trinoVersion)
                .setIssuedAt(new Date())
                .addClaims(extraCredentialsClaims)
                .serializeToJsonWith(new JacksonSerializer())
                .compact();

        Map<String, String> credentials = new HashMap<>(session.getIdentity().getExtraCredentials());
        credentials.put(OAuth2Properties.JWT_TOKEN_TYPE, subjectJwt);

        Map<String, String> properties = Map.of(
                "user", session.getUser(),
                "source", session.getSource().orElse(""),
                "trinoCatalog", catalogName.toString(),
                "trinoVersion", trinoVersion);

        return new SessionCatalog.SessionContext(sessionId, identity, credentials, properties);
    }

    private TableIdentifier toIdentifier(SchemaTableName schemaTableName)
    {
        return TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}
