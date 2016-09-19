/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.OutputHandler;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneScrubber {
    static {
        CassandraDaemon.initLog4j();
    }

    private static final String TOOL_NAME = "sstablescrub";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String DEBUG_OPTION = "debug";
    private static final String HELP_OPTION = "help";
    private static final String MANIFEST_CHECK_OPTION = "manifest-check";
    private static final String SKIP_CORRUPTED_OPTION = "skip-corrupted";
    private static final String NO_VALIDATE_OPTION = "no-validate";
    private static final String SSTABLES_OPTION = "sstables";
    private static final String PARALLEL_OPTION = "parallel";
    private static LeveledManifest manifest;

    public static void main(String args[]) {
        final Options options = Options.parseArgs(args);
        try {
            // load keyspace descriptions.
            DatabaseDescriptor.loadSchemas(false);

            if (Schema.instance.getCFMetaData(options.keyspaceName, options.cfName) == null)
                throw new IllegalArgumentException(
                        String.format("Unknown keyspace/columnFamily %s.%s", options.keyspaceName, options.cfName));

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);
            String snapshotName = "pre-scrub-" + System.currentTimeMillis();

            final OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);

            List<SSTableReader> sstables = new ArrayList<SSTableReader>();

            // Scrub sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet()) {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA))
                    continue;

                if (options.sstables.size() > 0) { // list of sstables is available
                    String filename = entry.getKey().filenameFor(Component.DATA);
                    if (!options.sstables.contains(filename)) { // filename doesn't match the given list of sstables,
                                                                // skip
                        if (options.debug) {
                            System.out.println("Skipping sstable not in list: " + filename);
                        }
                        continue;
                    }
                }

                try {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs.metadata);
                    sstables.add(sstable);

                    File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                    sstable.createLinks(snapshotDirectory.getPath());

                } catch (Exception e) {
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }
            System.out.println(String.format("Pre-scrub sstables snapshotted into snapshot %s", snapshotName));

            // if old-style json manifest, snapshot it
            if (cfs.directories.tryGetLeveledManifest() != null) {
                cfs.directories.snapshotLeveledManifest(snapshotName);
                System.out.println(String.format("Leveled manifest snapshotted into snapshot %s", snapshotName));
            }

            manifest = null;
            // If leveled, load the manifest
            if (cfs.getCompactionStrategy() instanceof LeveledCompactionStrategy) {
                int maxSizeInMB = (int) ((cfs.getCompactionStrategy().getMaxSSTableBytes()) / (1024L * 1024L));
                manifest = LeveledManifest.create(cfs, maxSizeInMB, sstables);
            }

            if (!options.manifestCheckOnly) {
                ExecutorService executor = Executors.newFixedThreadPool(options.parallel_threads);
                List<Future<?>> scrubs = new ArrayList<>();
                for (final SSTableReader sstable : sstables) {
                    scrubs.add(executor.submit(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                Scrubber scrubber = new Scrubber(cfs, sstable, options.skipCorrupted,
                                        !options.noValidate, handler, true);
                                try {
                                    scrubber.scrub();
                                } finally {
                                    scrubber.close();
                                }

                                if (manifest != null) {
                                    if (scrubber.getNewInOrderSSTable() != null)
                                        manifest.add(scrubber.getNewInOrderSSTable());

                                    List<SSTableReader> added = scrubber.getNewSSTable() == null
                                            ? Collections.<SSTableReader> emptyList()
                                            : Collections.singletonList(scrubber.getNewSSTable());
                                    manifest.replace(Collections.singletonList(sstable), added);
                                }

                                // Remove the sstable (it's been copied by scrub and snapshotted)
                                sstable.markObsolete();
                                sstable.releaseReference();
                            } catch (Exception e) {
                                System.err.println(String.format("Error scrubbing %s: %s", sstable, e.getMessage()));
                                e.printStackTrace(System.err);
                            }
                        }
                    }));
                }
                executor.shutdown();
                for(Future<?> scrub:scrubs) {
                    scrub.get();
                }
            }

            // Check (and repair) manifest
            if (manifest != null)
                checkManifest(manifest);

            SSTableDeletingTask.waitForDeletions();
            System.exit(0); // We need that to stop non daemonized threads
        } catch (Exception e) {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void checkManifest(LeveledManifest manifest) {
        System.out.println(String.format("Checking leveled manifest"));
        for (int i = 1; i <= manifest.getLevelCount(); ++i)
            manifest.repairOverlappingSSTables(i);
    }

    private static class Options {
        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean manifestCheckOnly;
        public boolean skipCorrupted;
        public boolean noValidate;
        public List<String> sstables;
        public Integer parallel_threads;

        private Options(String keyspaceName, String cfName) {
            this.keyspaceName = keyspaceName;
            this.cfName = cfName;
            this.sstables = new ArrayList<>();
            this.parallel_threads = 1;
        }

        public static Options parseArgs(String cmdArgs[]) {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION)) {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length != 2) {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.manifestCheckOnly = cmd.hasOption(MANIFEST_CHECK_OPTION);
                opts.skipCorrupted = cmd.hasOption(SKIP_CORRUPTED_OPTION);
                opts.noValidate = cmd.hasOption(NO_VALIDATE_OPTION);
                if (cmd.hasOption(SSTABLES_OPTION)) {
                    opts.sstables.addAll(Arrays.asList(cmd.getOptionValue(SSTABLES_OPTION).split(",")));
                }
                if (cmd.hasOption(PARALLEL_OPTION)) {
                    opts.parallel_threads = Integer.valueOf(cmd.getOptionValue(PARALLEL_OPTION));
                }

                return opts;
            } catch (ParseException e) {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, CmdLineOptions options) {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static CmdLineOptions getCmdLineOptions() {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION, "display stack traces");
            options.addOption("v", VERBOSE_OPTION, "verbose output");
            options.addOption("h", HELP_OPTION, "display this help message");
            options.addOption("m", MANIFEST_CHECK_OPTION,
                    "only check and repair the leveled manifest, without actually scrubbing the sstables");
            options.addOption("s", SKIP_CORRUPTED_OPTION, "skip corrupt rows in counter tables");
            options.addOption("n", NO_VALIDATE_OPTION, "do not validate columns using column validator");
            options.addOption("t", SSTABLES_OPTION, true, "comma separated list of sstable data files to scrub");
            options.addOption("p", PARALLEL_OPTION, true,
                    "parallel scrub, provide number of threads to have in thread pool. If not specified, uses 1 thread.");
            return options;
        }

        public static void printUsage(CmdLineOptions options) {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Scrub the sstable for the provided column family.");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
