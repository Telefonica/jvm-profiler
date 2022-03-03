package com.uber.profiling.reporters;

import com.uber.profiling.ArgumentUtils;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.JsonUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HDFSOutputReporter implements Reporter {

    public final static String ARG_OUTPUT_PATH = "output";
    public final static String ARG_MODE = "mode";

    public final static String ARG_WASB_ACCOUNT_PATH = "wasb_account";
    public final static String ARG_WASB_KEY_PATH = "wasb_key";

    private static final AgentLogger logger = AgentLogger.getLogger(HDFSOutputReporter.class.getName());
    private FileSystem hdfs;
    private Path outputBasePath;
    private long maxFileMb = 128L * 1024L * 1024L;
    private Map<String, Pair<Path, FSDataOutputStream>> writers = new HashMap<>();
    private volatile boolean closed = false;

    @Override
    public void updateArguments(Map<String, List<String>> parsedArgs) {
        String outputPath = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_OUTPUT_PATH);
        if (!ArgumentUtils.needToUpdateArg(outputPath)) {
            throw new IllegalArgumentException(String.format("%s path argument for hdfs reporter is mandatory", ARG_OUTPUT_PATH));
        }

        try {
            outputBasePath = new Path(URI.create(outputPath));
            Configuration hdfsConfig = buildFileSystemConfiguration(parsedArgs);
            hdfs = FileSystem.get(outputBasePath.toUri(), hdfsConfig);
            String mode = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_MODE);
            if (ArgumentUtils.needToUpdateArg(mode) && mode.equalsIgnoreCase("overwrite")) {
                hdfs.delete(outputBasePath, true);
            }
        } catch (IOException e) {
            logger.warn("Unable to create HDFS jvm-profiler file system", e);
            throw new IllegalArgumentException("Unable to create HDFS jvm-profiler file system", e);
        }
    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        if (hdfs != null && !closed) {
            try {
                FSDataOutputStream writer = getFileWriter(profilerName);
                writer.writeChars(JsonUtils.serialize(metrics));
                writer.writeChars(System.lineSeparator());
            } catch (IOException e) {
                logger.warn("Unable to append metric data to HDFS jvm-profiler", e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                for (Pair<Path, FSDataOutputStream> writer : writers.values()) {
                    writer.getRight().flush();
                    writer.getRight().close();
                }
                hdfs.close();
                hdfs = null;
                writers = new HashMap<>();
                closed = true;
            } catch (IOException e) {
                logger.warn("Unable to close HDFS jvm-profiler reporter", e);
            }
        }

    }

    private Configuration buildFileSystemConfiguration(Map<String, List<String>> parsedArgs) {
        Configuration configuration = new Configuration();

        String wasbAccount = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_WASB_ACCOUNT_PATH);
        String wasbKey = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_WASB_KEY_PATH);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        if (ArgumentUtils.needToUpdateArg(wasbAccount) && ArgumentUtils.needToUpdateArg(wasbKey)) {
            configuration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
            configuration.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure");
            configuration.set(String.format("fs.azure.account.key.%s.blob.core.windows.net", wasbAccount), wasbKey);
        }

        return configuration;
    }

    private FSDataOutputStream getFileWriter(String profilerName) throws IOException {
        synchronized (this) {
            Pair<Path, FSDataOutputStream> chunkData = writers.get(profilerName);
            if (chunkData == null || needsRolling(chunkData.getRight())) {
                if (chunkData != null) {
                    chunkData.getRight().flush();
                    chunkData.getRight().close();
                }
                Path chunkPath = buildFilePath(profilerName, UUID.randomUUID().toString());
                logger.info(String.format("Rolling new file: %s", chunkPath));
                writers.put(profilerName, Pair.of(chunkPath, hdfs.create(chunkPath, true)));
                chunkData = writers.get(profilerName);
            } else {
                logger.debug(String.format("Using last file new file: %s", chunkData.getLeft()));
            }
            return chunkData.getRight();
        }
    }

    private Path buildFilePath(String profilerName, String chunk) {
        return new Path(String.format("%s/%s/%s.json", outputBasePath.toString(), profilerName, chunk));
    }

    private boolean needsRolling(FSDataOutputStream chunk) {
        return chunk.size() > maxFileMb || chunk.size() == Integer.MAX_VALUE;
    }

}
