package playground;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.ArrayList;

public class tryParsArgs {
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");

        parser.addArgument("action").type(String.class)
                .choices("upload", "download", "delete", "getversion")
                .help("Action -- upload, download, delete, getversion");
        parser.addArgument("target_file").type(String.class)
                .help("filename for action");

        parser.addArgument("destination_directory").type(String.class)
                .help("destination directory for download action only")
                .nargs("*")
                .setDefault(new ArrayList<String>() {{
                    add("/Users/rongjinqiao/Codes/18sp/CSE291C/p2-rjqiao/download");
                }});

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
                throw new RuntimeException("Argument parsing failed");
            }
            System.out.println(c_args);
            System.out.println(c_args.get("destination_directory").getClass().getName());
            System.out.println(c_args.getList("destination_directory"));
//            System.out.println(c_args.getString("destination_directory").getClass().getName());
//            System.out.println(c_args.getString("destination_directory").equals("[/aaa]"));
        } catch (RuntimeException e) {
//            logger.severe(e.toString());
            e.printStackTrace();
        }


    }
}
