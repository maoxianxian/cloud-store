package surfstore;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import myUtils.BlockUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    {
//        logger.setLevel(Level.FINE);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINER);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    protected Server server;
    static protected ConfigReader config;
    static protected int currentNum;
    static protected int leaderNum;




    // blockChannel and blockStub is not shutdown
    // should shut down in stop() ?
    static private ManagedChannel blockChannel = null;
    static private BlockStoreGrpc.BlockStoreBlockingStub blockStub = null;


    static private MetadataStoreGrpc.MetadataStoreBlockingStub leaderMetadataStub;
//    private final MetadataStoreGrpc.MetadataStoreBlockingStub currentMetadataStub;

    static private ArrayList<ManagedChannel> metadataChannelList;
    static private ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubList;

//    static private ArrayList<Integer> last_


    public MetadataStore(ConfigReader config, int number) {
        MetadataStore.config = config;
        MetadataStore.currentNum = number - 1;
        MetadataStore.leaderNum = config.getLeaderNum() - 1;


//        logger.fine("config: "+config);
        logger.fine("currentNum: " + MetadataStore.currentNum);
        logger.fine("leaderNum: " + MetadataStore.leaderNum);

        // do not need to init blockStub or metadataStubList if not leader
        if (leaderNum == currentNum) {
            MetadataStore.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                    .usePlaintext(true).build();
            MetadataStore.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

            MetadataStore.metadataChannelList = new ArrayList<>();
            MetadataStore.metadataStubList = new ArrayList<>();
            for (int i = 0; i < config.numMetadataServers; i++) {
                ManagedChannel mc0 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i + 1))
                        .usePlaintext(true).build();
                MetadataStore.metadataChannelList.add(mc0);

                MetadataStoreGrpc.MetadataStoreBlockingStub ms0 = MetadataStoreGrpc.newBlockingStub(mc0);
                MetadataStore.metadataStubList.add(ms0);
            }
            logger.fine(Integer.toString(metadataStubList.size()));

            MetadataStore.leaderMetadataStub = MetadataStore.metadataStubList.get(MetadataStore.leaderNum);


        }

    }


    // do not know where to use it
    public void shutdown() throws InterruptedException {
        if (currentNum == leaderNum) {
            blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

            for (ManagedChannel mc : MetadataStore.metadataChannelList) {
                mc.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    public void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null) {
            //throw new RuntimeException("Argument parsing failed");
            return;
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config, c_args.getInt("number"));
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }


    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        // Fields declaration
        private Map<String, FileInfo> fileBlockHashMap = new HashMap<>();
        // assert log.size()-1 >= last_applied_index
        private List<Log> logs = new ArrayList<>();
        private int last_applied_index = -1;
        private boolean crashed = false;
        private Semaphore sem = new Semaphore(1);
        private Semaphore sem_upload_delete = new Semaphore(1);
        private Thread heartbeatThread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
//                        logger.info("To send heartbeat");
                        sleep(500);
                        sem.acquire();
                        for (int i = 0; i < metadataStubList.size(); i++) {
                            if (i == currentNum || i == leaderNum) {
                                continue;
                            }
                            MetadataStoreGrpc.MetadataStoreBlockingStub ms = metadataStubList.get(i);
                            HeartbeatReply heartbeatReply = ms.heartbeat(Empty.newBuilder().build());
//                            logger.fine("heartbeatReply.getLastCommit() == " + heartbeatReply.getLastCommit());
                            if (heartbeatReply.getIsCrashed()) {
                                continue;
                            } else if (heartbeatReply.getLastCommit() > logs.size() - 1) {
                                throw new RuntimeException("Wrong Log Index");
                            } else if (heartbeatReply.getLastCommit() < logs.size() - 1) {
                                // call appendLogEntries rpc
                                LogList.Builder logListBuilder = LogList.newBuilder();
                                logListBuilder.addAllLoglist(logs.subList(heartbeatReply.getLastCommit() + 1, logs.size()));
                                logger.fine("Heartbeat update log to metadata server " + i + " !!!!!!");
                                ms.appendLogEntries(logListBuilder.build());
                            } else {
                            }
                        }
//                        logger.info("Sent heartbeat");
                    } catch (InterruptedException e) {
                        logger.severe("Interrupted exception in Heartbeat thread");
                        System.exit(-1);
                    } catch (StatusRuntimeException e) {
                        logger.fine("Not able to heartbeat with some follower");
//                        System.exit(-1);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    } finally {
                        sem.release();
                    }

                }
            }
        };

        private MetadataStoreImpl() {
            super();
            if (isLeader()) {
                this.heartbeatThread.start();
            }
        }

        private boolean isLeader() {
            return MetadataStore.currentNum == MetadataStore.leaderNum;
        }

        private boolean isCrashed() {
            return this.crashed;
        }

        // TODO: private methods

        private void twoPhaseCommit(FileInfo fileInfo, WriteResult.Builder responseBuilder) throws InterruptedException {
            this.sem.acquire();

            int index = this.logs.size();
            Log.Builder logBuilder = Log.newBuilder();
            logBuilder.setIndex(index)
                    .setUpdateFile(fileInfo);
            Log log = logBuilder.build();

            int count = 1;
            for (int i = 0; i < metadataStubList.size(); i++) {
                if (i == currentNum || i == leaderNum) {
                    continue;
                }

                MetadataStoreGrpc.MetadataStoreBlockingStub ms = metadataStubList.get(i);
                SimpleAnswer simpleAnswer = ms.prepare(log);
                if (simpleAnswer.getAnswer()) {
                    count++;
                }
            }

            logger.fine("1 Phase commit count: " + count);

            if (count * 2 > metadataStubList.size()) {
                for (int i = 0; i < metadataStubList.size(); i++) {
                    if (i == currentNum || i == leaderNum) {
                        continue;
                    }
                    MetadataStoreGrpc.MetadataStoreBlockingStub ms = metadataStubList.get(i);
                    ms.commit(log);
                }
                // add log to logList
                // apply log

                // must be yes
                if (log.getIndex() == this.logs.size()) {
                    logs.add(log);
                    this.applyLog(log);
                } else {
                    logger.severe("More log with same index");
                    System.exit(-2);
                }


                responseBuilder.setResult(WriteResult.Result.OK).setCurrentVersion(fileInfo.getVersion());
            } else {
                for (int i = 0; i < metadataStubList.size(); i++) {
                    if (i == currentNum || i == leaderNum) {
                        continue;
                    }
                    MetadataStoreGrpc.MetadataStoreBlockingStub ms = metadataStubList.get(i);
                    ms.abort(log);
                }
                // build response
                responseBuilder.setResult(WriteResult.Result.NOT_LEADER);
                logger.info("ABORT!!!!!!");
                // "Abort" is out of expectation
                // just crach the server
                System.exit(-1);
            }
            this.sem.release();
        }

        // assert log.index > this.last_applied_index
        private boolean applyLog(Log log) {
            logger.fine("ApplyLog: index=" + log.getIndex() + "\t");
            FileInfo newFileInfo = log.getUpdateFile();
            FileInfo oldFileInfo = this.fileBlockHashMap.getOrDefault(newFileInfo.getFilename(),
                    FileInfo.newBuilder().setVersion(0).setFilename(newFileInfo.getFilename()).build());
            if (newFileInfo.getVersion() != oldFileInfo.getVersion() + 1) {
                logger.severe("Not able to apply log");
                return false;
            } else {
                this.fileBlockHashMap.put(newFileInfo.getFilename(), newFileInfo);
                this.last_applied_index = log.getIndex();
                return true;
            }
        }


        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {

            logger.info("Ping from Client. [MetadataStore]");

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        // filename: String
        // filename does not exist in fileBlockHashMap -> version(0) return
        // filename exists
        //    1. deleted
        //    2. blocks exists
        @Override
        public void readFile(FileInfo request,
                             io.grpc.stub.StreamObserver<FileInfo> responseObserver) {
            logger.info("Reading file with filename: " + request.getFilename());

//            if(!isLeader()){
//                responseBuilder.setResult(WriteResult.Result.NOT_LEADER);
//                WriteResult response = responseBuilder.build();
//                responseObserver.onNext(response);
//                responseObserver.onCompleted();
//                return;
//            }

//            assert request.getFilename() != null;
//            assert request.getBlocklistList() != null;

            FileInfo response = null;
            String filename = request.getFilename();
            if (fileBlockHashMap.containsKey(filename)) {
                response = fileBlockHashMap.get(request.getFilename());
            } else {
                logger.fine("File not found");
                response = FileInfo.newBuilder().setVersion(0).setFilename(filename).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(FileInfo request,
                               io.grpc.stub.StreamObserver<WriteResult> responseObserver) {
            logger.info("Modifying file with filename: " + request.getFilename());

            WriteResult.Builder responseBuilder = WriteResult.newBuilder();

            if (!isLeader()) {
                responseBuilder.setResult(WriteResult.Result.NOT_LEADER);
                WriteResult response = responseBuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

//            assert request.getFilename() != null;
//            assert request.getBlocklistList() != null;

//            List<String> blockHashList = new ArrayList<>(request.getBlocklistList());


            List<String> missingBlockHashList = new ArrayList<>();

            // find missing blocks in fileBlockHashMap
            for (String blockHash : request.getBlocklistList()) {
                SimpleAnswer sa = MetadataStore.blockStub.hasBlock(Block.newBuilder().setHash(blockHash).build());
                if (!sa.getAnswer()) {
                    missingBlockHashList.add(blockHash);
                }
            }

            String filename = request.getFilename();
            int currentVersion = 0;
            FileInfo fi = null;
            try{
                sem_upload_delete.acquire();
            } catch (InterruptedException e){
                e.printStackTrace();
                System.exit(-1);
            }
            if (fileBlockHashMap.containsKey(filename)) {
                fi = fileBlockHashMap.get(filename);
                currentVersion = fi.getVersion();
            }

            // if not exist, create
            if (request.getVersion() != currentVersion + 1) {
                responseBuilder.setResultValue(1).setCurrentVersion(currentVersion);
            } else if (missingBlockHashList.isEmpty()) {
                // update metadata and build response

                FileInfo newFileInfo =
                        FileInfo.newBuilder()
                                .setFilename(filename)
                                .setVersion(currentVersion + 1)
                                .addAllBlocklist(request.getBlocklistList())
                                .build();

                // TODO: Two-Phase Commit
                logger.fine("Before 2PC");
                try {
                    this.twoPhaseCommit(newFileInfo, responseBuilder);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    responseObserver.onError(Status.INTERNAL.withDescription("Semaphore Error in 2PC").asException());
                    System.exit(-1);
                }


//                fileBlockHashMap.put(filename, newFileInfo);
                logger.fine("Modified! New file version: " + (currentVersion + 1));
//                responseBuilder.setResultValue(0).setCurrentVersion(currentVersion + 1);
            } else {
                responseBuilder.setResultValue(2)
                        .setCurrentVersion(currentVersion)
                        .addAllMissingBlocks(missingBlockHashList);
            }

            sem_upload_delete.release();

            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // filename: String
        // filename does not exist in fileBlockHashMap -> version(0) return
        // filename exists
        //    1. deleted
        //    2. blocks exists
        @Override
        public void deleteFile(FileInfo request,
                               io.grpc.stub.StreamObserver<WriteResult> responseObserver) {
            logger.info("Deleting file with filename: " + request.getFilename());
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();

            if (!isLeader()) {
                responseBuilder.setResult(WriteResult.Result.NOT_LEADER);
                WriteResult response = responseBuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            String filename = request.getFilename();
            int currentVersion = 0;
            FileInfo fi = null;

            try{
                sem_upload_delete.acquire();
            } catch (InterruptedException e){
                e.printStackTrace();
                System.exit(-1);
            }

            if (fileBlockHashMap.containsKey(filename)) {
                fi = fileBlockHashMap.get(filename);
                currentVersion = fi.getVersion();
                if (request.getVersion() != currentVersion + 1) // request version not correct
                {
                    logger.info("Version wrong");
                    responseBuilder.setResultValue(1).setCurrentVersion(currentVersion);
                } else if (BlockUtils.blockListIsDeleted(fi.getBlocklistList())) {
                    // previously deleted, do not modify version -- return version 0, result OK
                    logger.info("Had been deleted");
                    responseBuilder.setCurrentVersion(0).setResult(WriteResult.Result.OK);
                } else {
                    // update metadata and build response
                    // delete metadata, blockList only one block with hash "0"
                    // TODO: Two-Phase Commit
                    FileInfo newFileInfo =
                            FileInfo.newBuilder()
                                    .setFilename(filename)
                                    .setVersion(currentVersion + 1)
                                    .addBlocklist(BlockUtils.EMPTY_HASH)
                                    .build();

                    // TODO: Two-Phase Commit
                    try {
                        this.twoPhaseCommit(newFileInfo, responseBuilder);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        responseObserver.onError(Status.INTERNAL.withDescription("Semaphore Error in 2PC").asException());
                        System.exit(-1);
                    }

//                    fileBlockHashMap.put(filename, newFileInfo);
                    logger.info("Deleted! new file version: " + (currentVersion + 1));
//                    responseBuilder.setResultValue(0).setCurrentVersion(currentVersion + 1);
                }
            } else // does not exist -- return version 0, result OK
            {
                logger.info("Not found delete file");
                responseBuilder.setCurrentVersion(0).setResult(WriteResult.Result.OK);
            }

            sem_upload_delete.release();


            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isLeader(Empty request,
                             io.grpc.stub.StreamObserver<SimpleAnswer> responseObserver) {
            logger.info("Is Leader?");

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.isLeader()).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(Empty request,
                          io.grpc.stub.StreamObserver<Empty> responseObserver) {
            logger.info("Crash!!");

            this.crashed = true;

            Empty response = Empty.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(Empty request,
                            io.grpc.stub.StreamObserver<Empty> responseObserver) {
            logger.info("Restore!!");

            this.crashed = false;

            Empty response = Empty.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(Empty request,
                              io.grpc.stub.StreamObserver<SimpleAnswer> responseObserver) {
            logger.info("Is Crashed?");


            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.isCrashed()).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(FileInfo request,
                               io.grpc.stub.StreamObserver<FileInfo> responseObserver) {
            logger.info("Getting file version with filename: " + request.getFilename());
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();

            String filename = request.getFilename();
            int currentVersion = 0;
            FileInfo fi = null;
            if (filename != null && fileBlockHashMap.containsKey(filename)) {
                fi = fileBlockHashMap.get(filename);
                currentVersion = fi.getVersion();
            }

            FileInfo response =
                    FileInfo.newBuilder()
                            .setFilename(filename)
                            .setVersion(currentVersion)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }

        @Override
        public void heartbeat(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.HeartbeatReply> responseObserver) {
            HeartbeatReply.Builder responseBuilder = HeartbeatReply.newBuilder();
//            logger.info("Receive heartbeat");
            if (this.isCrashed()) {
                responseBuilder.setIsCrashed(true);
            } else {
                try {
                    sem.acquire();
                    responseBuilder.setIsCrashed(false)
                            .setLastCommit(this.last_applied_index);
                    sem.release();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    responseObserver.onError(Status.INTERNAL.withDescription("Semaphore error in commit").asException());
                    System.exit(-1);
                }
            }
            HeartbeatReply response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // Simply apply log from
        @Override
        public void appendLogEntries(surfstore.SurfStoreBasic.LogList request,
                                     io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            logger.info("Append Entries");
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            if (this.isCrashed()) {
                builder.setAnswer(false);
            } else {
                try {
                    this.sem.acquire();
                    // apply many logs
                    // should not be empty
                    List<Log> logListToUpdate = request.getLoglistList();
                    if (logListToUpdate.isEmpty()) {
                        throw new RuntimeException("Empty Append Log");
                    } else {
                        int start = this.last_applied_index + 1;
                        this.logs = this.logs.subList(0, start);
                        if (logListToUpdate.get(0).getIndex() < start) {
                            throw new RuntimeException("Wrong Index when appending entries");
                        }
                        for (Log log : logListToUpdate) {
                            if (log.getIndex() < start) {
                                continue;
                            }
                            this.logs.add(log);
                            this.applyLog(log);
                        }


                    }
                    this.sem.release();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    responseObserver.onError(Status.INTERNAL.withDescription("Semaphore error in commit").asException());
                    System.exit(-1);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-2);
                }
            }
            responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void prepare(surfstore.SurfStoreBasic.Log request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            if (this.isCrashed()) {
                responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(false).build());
                responseObserver.onCompleted();
                return;
            }
            try {
                logger.fine("this.logs.size() == " + this.logs.size() +
                        "\nrequest.getIndex() == " + request.getIndex());
                boolean answer = false;
                this.sem.acquire();
                if (this.logs.size() == request.getIndex()) {
                    answer = true;
                    this.logs.add(request);
                } else {
                    answer = false;
                }
                this.sem.release();
                responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(answer).build());
                responseObserver.onCompleted();
            } catch (InterruptedException e) {
                e.printStackTrace();
                responseObserver.onError(Status.INTERNAL.withDescription("Semaphore error in commit").asException());
                System.exit(-1);
            }


        }

        @Override
        public void commit(surfstore.SurfStoreBasic.Log request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            if (this.isCrashed()) {
                builder.setAnswer(false);
            } else {
                try {
                    this.sem.acquire();
                    // apply request
                    if (request.getIndex() == this.last_applied_index + 1) {
//                        this.logs.add(request);
                        this.applyLog(request);
                    }
                    this.sem.release();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    responseObserver.onError(Status.INTERNAL.withDescription("Semaphore error in commit").asException());
                    System.exit(-1);
                }
            }
            responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(true).build());
            responseObserver.onCompleted();
        }

        // should not be called!!
        @Override
        public void abort(surfstore.SurfStoreBasic.Log request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            try {
                this.sem.acquire();
                if (this.logs.size() >= request.getIndex() + 1) {
                    this.logs = this.logs.subList(0, request.getIndex());
                    logger.severe("Abort in follower");
                }
                this.sem.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
                responseObserver.onError(Status.INTERNAL.withDescription("Semaphore error in commit").asException());
                System.exit(-1);
            }
            responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(true).build());
            responseObserver.onCompleted();
        }
    }
}