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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.http.HttpClientBuilder;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.catalog.nessie.AuthenticationType.BASIC;
import static io.trino.plugin.iceberg.catalog.nessie.AuthenticationType.BEARER;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergNessieCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(NessieConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(NessieIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoNessieCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoCatalogFactory.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static NessieIcebergClient createNessieIcebergClient(NessieConfig config)
    {
        HttpClientBuilder builder = HttpClientBuilder.builder()
                .withUri(config.getServerUri())
                .withDisableCompression(!config.isCompressionEnabled());

        if (config.getReadTimeoutMillis().isPresent()) {
            builder.withReadTimeout(config.getReadTimeoutMillis().get());
        }
        if (config.getConnectTimeoutMillis().isPresent()) {
            builder.withConnectionTimeout(config.getConnectTimeoutMillis().get());
        }
        if (config.getAuthenticationType().isPresent()) {
            AuthenticationType type = config.getAuthenticationType().get();
            if (type.equals(BASIC) && config.getUsername().isPresent() && config.getPassword().isPresent()) {
                builder.withAuthentication(BasicAuthenticationProvider.create(config.getUsername().get(), config.getPassword().get()));
            }
            else if (type.equals(BEARER) && config.getBearerToken().isPresent()) {
                builder.withAuthentication(BearerAuthenticationProvider.create(config.getBearerToken().get()));
            }
        }
        return new NessieIcebergClient(builder.build(NessieApiV1.class), config.getDefaultReferenceName(), null);
    }
}
