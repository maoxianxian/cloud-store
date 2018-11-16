package surfstore;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import myUtils.BlockUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.SimpleAnswer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public final class BlockStore {
    private static final Logger logger = Logger.getLogger(BlockStore.class.getName());
    {
        logger.setLevel(Level.FINE);
        ConsoleHandler handler = new ConsoleHandler();
        // PUBLISH this level
        handler.setLevel(Level.FINER);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    protected Server server;
    protected ConfigReader config;

    public BlockStore(ConfigReader config) {
        this.config = config;
    }

    protected void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BlockStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("BlockStore").build()
                .description("BlockStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
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

        final BlockStore server = new BlockStore(config);
        server.start(config.getBlockPort(), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class BlockStoreImpl extends BlockStoreGrpc.BlockStoreImplBase {

        protected Map<String, byte[]> blockMap;

        public BlockStoreImpl() {
            super();
            this.blockMap = new HashMap<>();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            logger.info("Ping from Client. [BlockStore]");
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!
        @Override
        public void storeBlock(surfstore.SurfStoreBasic.Block request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            logger.info("Storing block with hash " + request.getHash());

//            assert request.getHash() != null;
//            assert request.getData() != null;

            this.blockMap.put(request.getHash(), request.getData().toByteArray());
//            logger.fine(request.getData().toString());

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // if hash exists in map -- return block
        // not exist -- hash = blockUtils.EMPTY
        public void getBlock(surfstore.SurfStoreBasic.Block request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Block> responseObserver) {
            logger.info("Getting block with hash " + request.getHash());

//            assert request.getHash() != null;

            Block.Builder builder = Block.newBuilder();

            Block response = null;

            if (blockMap.containsKey(request.getHash())) {
                logger.fine("Block exist");
                byte[] data = blockMap.get(request.getHash());

                builder.setData(ByteString.copyFrom(data));
                builder.setHash(request.getHash());
                response = builder.build();
            } else {
                // block not exist
                logger.fine("Block does not exist");
                response = builder.setHash(BlockUtils.EMPTY_HASH).build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public void hasBlock(surfstore.SurfStoreBasic.Block request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            logger.info("Testing for existence of block with hash " + request.getHash());

//            assert request.getHash() != null;

            boolean answer = blockMap.containsKey(request.getHash());
            logger.fine("Block exists? " + answer);

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(answer).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
