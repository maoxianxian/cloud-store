package surfstore;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import myUtils.BlockUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    {
        logger.setLevel(Level.FINE);
        ConsoleHandler handler = new ConsoleHandler();
        // PUBLISH this level
        handler.setLevel(Level.FINER);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

//    private final ManagedChannel metadataChannel;
//    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final MetadataStoreGrpc.MetadataStoreBlockingStub leaderMetadataStub;
//    private final MetadataStoreGrpc.MetadataStoreBlockingStub currentMetadataStub;

    private final ArrayList<ManagedChannel> metadataChannelList;
    private final ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubList;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    // command line args

    // no slash in "getversion", "delete" and "download"
    // with slash in "upload"
    private final String targetFileName;

    private final String action;
    private final String dest_dir; // only works with "download" action
    private final int leaderNum;


    // original
//    public Client(ConfigReader config) {
//        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
//                .usePlaintext(true).build();
//        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
//
//        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
//                .usePlaintext(true).build();
//        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
//
//        this.config = config;
//    }

    public Client(ConfigReader config, String action, String targetFileName, String dest_dir) {
        this.metadataChannelList = new ArrayList<>();
        this.metadataStubList = new ArrayList<>();
        this.leaderNum = config.getLeaderNum() - 1;
        for (int i = 0; i < config.numMetadataServers; i++) {
            ManagedChannel mc0 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i + 1))
                    .usePlaintext(true).build();
            this.metadataChannelList.add(mc0);
            MetadataStoreGrpc.MetadataStoreBlockingStub ms0 = MetadataStoreGrpc.newBlockingStub(mc0);
            this.metadataStubList.add(ms0);
        }
        this.leaderMetadataStub = this.metadataStubList.get(this.leaderNum);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        // command line arguments
        this.targetFileName = targetFileName;
        this.action = action;
        this.dest_dir = dest_dir;
    }

    public void shutdown() throws InterruptedException {
        for (ManagedChannel mc : this.metadataChannelList) {
            mc.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
//        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    // true  -> OK
    // false -> Not Found
    private boolean upload()
            throws InterruptedException, StatusRuntimeException {
        String pathName = this.targetFileName;
        logger.info("Uploading File: " + pathName);
        // must have slash
        /*
        if (!this.targetFileName.contains("/")) {
            logger.info("Invalid filename format ( does not have slash)");
            return false;
        }
        */
        File targetFile = new File(pathName);
        if (!BlockUtils.existNotDir(targetFile)) {
//            throw new BlockUtils.NoLocalTargetFileException(filename);
            logger.info("No such file locally");
            return false;
        }
        String filename = targetFile.getName();
        if (filename.equals("")) {
            logger.info("filename null");
            return false;
        }


        List<Block> blockList = BlockUtils.file2blocks(targetFile);
        List<String> blockHashList = new ArrayList<>();
        Map<String, Block> blockMap = new HashMap<>();
        for (Block block : blockList) {
            blockHashList.add(block.getHash());
            blockMap.put(block.getHash(), block);
//            logger.fine("missing Block: \n" + block.getHash());
        }

        int currentVersion = this.leaderMetadataStub
                .getVersion(FileInfo.newBuilder().setFilename(filename).build()).getVersion();

        while (true) {
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(filename).setVersion(currentVersion + 1).addAllBlocklist(blockHashList);
            WriteResult wr = this.leaderMetadataStub.modifyFile(builder.build());
            if (wr.getResult().equals(WriteResult.Result.OK)) {
                logger.info("Uploaded: " + filename +
                        "\n\tmy version: " + (currentVersion + 1) +
                        "\n\tserver version: " + wr.getCurrentVersion());
                logger.info("Successfully upload: " + filename);
                break;
            } else if (wr.getResult().equals(WriteResult.Result.OLD_VERSION)) {
                logger.info("Old Version: " + filename +
                        "\n\tmy version: " + (currentVersion + 1) +
                        "\n\tserver version: " + wr.getCurrentVersion());
//                return false;
                currentVersion = wr.getCurrentVersion();
            } else if (wr.getResult().equals(WriteResult.Result.MISSING_BLOCKS)) {
                for (String missingBlockHash : wr.getMissingBlocksList()) {
                    if (!blockMap.containsKey(missingBlockHash)) {
                        logger.severe("Receive unknown blockHash from metadataServer");
                        return false;
                    }
                    this.blockStub.storeBlock(blockMap.get(missingBlockHash));
                    logger.fine("upload missing Block: \n" + missingBlockHash);
                }
            } else {
                logger.info("Unknown issue when uploading");
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        return true;
    }

    private boolean download() {
        String filename = this.targetFileName;
        logger.info("Downloading File: " + filename + "  \tTo destination directory: " + this.dest_dir);
        if (filename.contains("/")) {
            logger.info("Invalid filename format (has slash)");
            return false;
        }
        File dir = new File(this.dest_dir);
        if (!dir.isDirectory()) {
            logger.info("Destination directory not found");
        }

        String pathname = new File(this.dest_dir).getAbsolutePath() + "/" + filename;

        // Get all blocks in current directory
        Map<String, Block> blockMap = new HashMap<>();
        // If this pathname does not denote a directory, then listFiles() returns null. 
        File[] files = dir.listFiles();

        for (File file : files) {
            if (file.isFile()) {
                List<Block> tempBlockList = BlockUtils.file2blocks(file);
                for (Block block : tempBlockList) {
                    blockMap.put(block.getHash(), block);
                }
            }
        }

        // download and overwrite exist file
        FileInfo fi = FileInfo.newBuilder().setFilename(filename).build();

        fi = this.leaderMetadataStub.readFile(fi);

        if (fi.getVersion() == 0) {
            logger.info("File not exist");
            return false;
        } else if (BlockUtils.blockListIsDeleted(fi.getBlocklistList())) {
            logger.info("File was deleted");
            return false;
        } else {
            try {
//                logger.fine("Download Version");
//                 file exists and download it
                List<Block> blockList = new ArrayList<>();
                logger.fine("get blocks number: " + fi.getBlocklistList().size());
                for (String blockHash : fi.getBlocklistList()) {
                    if (blockMap.containsKey(blockHash)) {
                        blockList.add(blockMap.get(blockHash));
                        continue;
                    }
                    Block block = Block.newBuilder().setHash(blockHash).build();
                    if (!this.blockStub.hasBlock(block).getAnswer()) {
                        throw new BlockUtils.BlockHashNotFoundException();
                    }
                    block = this.blockStub.getBlock(block);
//                     block hash does not exist or ???empty block???
//                    if (block.getHash().equals(BlockUtils.EMPTY_HASH)) {
//                        throw new BlockUtils.BlockHashNotFoundException();
//                    }
                    blockList.add(block);
                    // Add the newest block
                    blockMap.put(blockHash, block);
                }
                byte[] bytes = BlockUtils.concatBlocks(blockList);
                FileOutputStream stream = new FileOutputStream(pathname);  // throw file not found
                stream.write(bytes);
                stream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                logger.info("Not able to write to the file: " + pathname);
                return false;
            } catch (BlockUtils.BlockHashNotFoundException e) {
                logger.severe("Not able to get block with hash -- hash not found in blockStore");
                e.printStackTrace();
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    private boolean delete() {
        String filename = this.targetFileName;
        logger.info("Deleting File: " + filename);
        if (filename.contains("/")) {
            logger.info("Invalid filename format (has slash)");
            return false;
        }
        FileInfo fi = FileInfo.newBuilder().setFilename(filename).build();
        int currentVersion = this.leaderMetadataStub.getVersion(fi).getVersion();
        if (currentVersion == 0) {
            logger.info("No such file: " + filename);
            return false;
        }

        fi = FileInfo.newBuilder().setFilename(filename).setVersion(currentVersion + 1).build();
        WriteResult wr = this.leaderMetadataStub.deleteFile(fi);
        if (wr.getCurrentVersion() == 0) {
            logger.info("Delete file that had been deleted");
            return false;
        } else if (wr.getResult().equals(WriteResult.Result.OK)) {
            logger.info("Successfully delete: " + filename);
            return true;
        } else if (wr.getResult().equals(WriteResult.Result.OLD_VERSION)) {
            logger.info("Old Version: " + filename +
                    "\n\tmy version: " + (currentVersion + 1) +
                    "\n\tserver version: " + wr.getCurrentVersion());
            return false;
        } else {
            logger.info("Unknown issue when deleting");
            return false;
        }
    }

    private List<Integer> getVersion() {
        String filename = this.targetFileName;
        logger.info("Getting version: " + filename);
        if (filename.contains("/")) {
            logger.info("Invalid filename format (has slash)");

            return new ArrayList<Integer>(Collections.nCopies(config.numMetadataServers, -1));
        }

        FileInfo fi = FileInfo.newBuilder().setFilename(filename).build();
//        FileInfo fi = this.leaderMetadataStub.readFile(fi0);
        ArrayList<Integer> res = new ArrayList<>();

        for (MetadataStoreGrpc.MetadataStoreBlockingStub ms : this.metadataStubList) {
            try {
                int currentVersion = ms.getVersion(fi).getVersion();
                res.add(currentVersion);
            } catch (RuntimeException e) {
                e.printStackTrace();
                res.add(-1);
            }
        }
        return res;
    }

    public boolean crash() {
        try {
            metadataStubList.get(metadataStubList.size() - 1).crash(Empty.newBuilder().build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean restore() {
        try {
            metadataStubList.get(metadataStubList.size() - 1).restore(Empty.newBuilder().build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void go() {
        leaderMetadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");


        // default the last metadata server
        MetadataStoreGrpc.MetadataStoreBlockingStub followerMetadataStub = metadataStubList.get(metadataStubList.size() - 1);


        Object res = null;
        try {
            if (this.action.equals("upload")) {
                res = upload();
            } else if (this.action.equals("download")) {
                res = download();
            } else if (this.action.equals("delete")) {
                res = delete();
            } else if (this.action.equals("getversion")) {
                res = getVersion();
            } else if (this.action.equals("crash")) {
                res = crash();
                logger.info("Crash follower");
            } else if (this.action.equals("restore")) {
                res = restore();
                logger.info("Restore follower");
            } else if (this.action.equals("follower_upload")) {
                FileInfo.Builder builder = FileInfo.newBuilder();
                WriteResult wr = followerMetadataStub.modifyFile(builder.build());
                ensure(wr.getResult() == WriteResult.Result.NOT_LEADER);
            } else if (this.action.equals("follower_download")) {
                FileInfo.Builder builder = FileInfo.newBuilder().setFilename("1.txt");
                FileInfo fi = followerMetadataStub.readFile(builder.build());
                ensure(fi.getFilename().equals("1.txt"));
            } else if (this.action.equals("follower_delete")) {
                FileInfo.Builder builder = FileInfo.newBuilder();
                WriteResult wr = followerMetadataStub.deleteFile(builder.build());
                ensure(wr.getResult() == WriteResult.Result.NOT_LEADER);
            } else {
                throw new BlockUtils.NoActionException();
            }


            if (res instanceof Boolean) {
                System.out.println((boolean) res ? "OK" : "Not Found");
            } else if (res instanceof List) {
//                String toPrint = Arrays.toString(((List<Integer>) res).toArray());
//                System.out.println(toPrint.substring(1,toPrint.length()-1));
                List<Integer> res2 = (List<Integer>) res;
                for (int i = 0; i < res2.size(); i++) {
                    System.out.print(res2.get(i));
                    if (i != res2.size() - 1) {
                        System.out.print(' ');
                    }
                }
                System.out.println();
            }
        } catch (BlockUtils.NoActionException e) {
            logger.severe(BlockUtils.NoActionException.MESSAGE);
//        } catch (BlockUtils.NoLocalTargetFileException e) {
//            logger.severe("No such file locally: " + e.targetFilename);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

        logger.info("Client work complete !!!!!");

        // TODO: Implement your client here
//        Block b1 = stringToBlock("block1");
//        Block b2 = stringToBlock("block2");
//
//        blockStub.hasBlock(b1).getAnswer();
//        ensure(blockStub.hasBlock(b1).getAnswer() == false);
//
//        blockStub.storeBlock(b1);
//        ensure(blockStub.hasBlock(b1).getAnswer() == true);
//
//        Block b1prime = blockStub.getBlock(b1);
//        ensure(b1prime.getHash().equals(b1.getHash()));
//        ensure(b1prime.getData().equals(b1.getData()));
//
//        logger.info("Test complete !!!!!");
    }

    /*
     * TODO: Add command line handling here
     */

    //    private Block stringToBlock(String s) {
//
//        Block.Builder builder = Block.newBuilder();
//
//        try {
//            builder.setData(ByteString.copyFrom(s, "UTF-8"));
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e);
//        }
//
//        builder.setHash(HashUtils.sha256(s));
//
//        return builder.build();
//    }
//
    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion failed!");
        }
        logger.fine("Assertion OK");
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");

        Subparsers subparsers = parser.addSubparsers().dest("action")
                                .title("ACTIONS")
                                .metavar("valid actions");
        Subparser uploadParser = subparsers.addParser("upload")
                .help("upload <target_file>\n");
        uploadParser.addArgument("target_file").type(String.class)
                .help("Filepath for upload");
        Subparser downloadParser = subparsers.addParser("download")
                .help("download <target_file> [destination_directory]\n");
        downloadParser.addArgument("target_file").type(String.class)
                .help("Saved filename for downloading");
        downloadParser.addArgument("destination_directory").type(String.class)
                .help("Directory for saving the downloaded file (Optional)")
                .nargs("?")
                .setDefault("./");
        Subparser deleteParser = subparsers.addParser("delete")
                .help("delete <target_file>\n");
        deleteParser.addArgument("target_file").type(String.class)
                .help("Filename for deletion");
        Subparser getVersionParser = subparsers.addParser("getversion")
                .help("getversion <target_file>\n");
        getVersionParser.addArgument("target_file").type(String.class)
                .help("Filename for version query");
        subparsers.addParser("crash").help("simulates crashes\n");
        subparsers.addParser("restore").help("simulates restore\n");
        subparsers.addParser("follower_upload").help("Test for follower upload\n");
        subparsers.addParser("follower_download").help("Test for follower download\n");
        subparsers.addParser("follower_delete").help("Test for follower delete\n");

        /*
        parser.addArgument("action").type(String.class)
                .choices("upload", "download", "delete", "getversion", "crash", "restore", "follower_upload", "follower_download", "follower_delete")
                .help("Action -- upload, download, delete, getversion");
        parser.addArgument("target_file").type(String.class)
                .help("filename for action");

        parser.addArgument("destination_directory").type(String.class)
                .help("destination directory for download action only")
                .nargs("*")
                .setDefault(new ArrayList<String>() {{
                    add("./download");
                }});
         */

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) {
        Namespace c_args = parseArgs(args);
        try {
            if (c_args == null) {
                //throw new RuntimeException("Argument parsing failed");
                return;
            }
        } catch (RuntimeException e) {
//            logger.severe(e.toString());
            e.printStackTrace();
            return;
        }

        logger.config("CommandLine Arguments: " + c_args);

        File configf = new File(c_args.getString("config_file"));
        String targetFileName = c_args.getString("target_file");
        String action = c_args.getString("action");
        String dest_dir = c_args.getString("destination_directory");


        try {
            ConfigReader config = new ConfigReader(configf);
            // init client and stubs (metadataStore and blockStore)
            Client client = new Client(config, action, targetFileName, dest_dir);
            try {
                client.go();
            } finally {
                client.shutdown();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
