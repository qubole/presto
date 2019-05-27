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
package com.facebook.presto.hive.acid;

import com.facebook.presto.hive.DeleteDeltaLocations;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeName;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.orc.ACIDOrcPageSourceFactory;
import com.facebook.presto.hive.orc.DeletedRowsRegistry;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public class AcidPageProcessorProvider
{
    public static final HiveClientConfig CONFIG = new HiveClientConfig()
            .setParquetOptimizedReaderEnabled(true)
            .setUseOrcColumnNames(true);
    public static final ConnectorSession SESSION = new TestingConnectorSession(new HiveSessionProperties(CONFIG, new OrcFileWriterConfig())
            .getSessionProperties());
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment(new HiveHdfsConfiguration(new HdfsConfigurationUpdater(CONFIG)), CONFIG, new NoHdfsAuthentication());

    private AcidPageProcessorProvider()
    {
    }

    // Return ACID page source which reads only the data columns from ACID file and return isValid block additionaly
    public static ConnectorPageSource getAcidPageSource(String fileName, List<String> columnNames, List<Type> columnTypes)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, TupleDomain.all(), false);
    }

    public static ConnectorPageSource getAcidPageSource(String fileName, List<String> columnNames, List<Type> columnTypes, Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, TupleDomain.all(), deleteDeltaLocations, false);
    }

    // Return PageSource that reads the underlying ACID file as is
    public static ConnectorPageSource getActualPageSourceForAcidFile(String fileName, List<String> columnNames, List<Type> columnTypes)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, TupleDomain.all(), true);
    }

    public static ConnectorPageSource getActualPageSourceForAcidFile(String fileName, List<String> columnNames, List<Type> columnTypes, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, tupleDomain, true);
    }

    public static ConnectorPageSource getAcidPageSource(String fileName, List<String> columnNames, List<Type> columnTypes, TupleDomain<HiveColumnHandle> tupleDomain, boolean getActualPageSource)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, tupleDomain, Optional.empty(), getActualPageSource);
    }

    public static ConnectorPageSource getAcidPageSource(
            String fileName,
            List<String> columnNames,
            List<Type> columnTypes,
            TupleDomain<HiveColumnHandle> tupleDomain,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            boolean getActualPageSource)
    {
        File targetFile = new File((Thread.currentThread().getContextClassLoader().getResource(fileName).getPath()));
        ImmutableList.Builder<HiveColumnHandle> builder = ImmutableList.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            Type columnType = columnTypes.get(i);
            builder.add(new HiveColumnHandle(
                    columnNames.get(i),
                    toHiveType(new HiveTypeTranslator(), columnType),
                    columnType.getTypeSignature(),
                    0,
                    REGULAR,
                    Optional.empty()));
        }
        List<HiveColumnHandle> columns = builder.build();

        OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, true, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());
        ACIDOrcPageSourceFactory pageSourceFactory = new ACIDOrcPageSourceFactory(TYPE_MANAGER, CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats(), orcPageSourceFactory);

        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        return pageSourceFactory.createPageSource(
                config,
                SESSION,
                new Path(targetFile.getAbsolutePath()),
                0,
                targetFile.length(),
                targetFile.length(),
                createSchema(HiveStorageFormat.ORC, columnNames, columnTypes),
                columns,
                tupleDomain,
                DateTimeZone.forID(SESSION.getTimeZoneKey().getId()),
                deleteDeltaLocations,
                getActualPageSource).get();
    }

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(typeTranslator, type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        schema.setProperty(TABLE_IS_TRANSACTIONAL, "true");
        return schema;
    }

    public static DeletedRowsRegistry createDeletedRowsRegistry(Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        OrcPageSourceFactory pageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, true, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());

        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        return new DeletedRowsRegistry(
                pageSourceFactory,
                SESSION,
                config,
                DateTimeZone.forID(SESSION.getTimeZoneKey().getId()),
                HDFS_ENVIRONMENT,
                CONFIG.getDeleteDeltaCacheSize(),
                CONFIG.getDeleteDeltaCacheTTL(),
                deleteDeltaLocations);
    }

    public static void addNationTableDeleteDeltas(DeleteDeltaLocations deleteDeltaLocations, long minWriteId, long maxWriteId, int statementId)
    {
        // ClassLoader finds top level resources, find that and build delta locations from it
        File partitionLocation = new File((Thread.currentThread().getContextClassLoader().getResource("nation_delete_deltas").getPath()));
        Path deleteDeltaPath = new Path(new Path(partitionLocation.toString()), AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, statementId));
        deleteDeltaLocations.addDeleteDelta(deleteDeltaPath, minWriteId, maxWriteId, statementId);
    }
}
