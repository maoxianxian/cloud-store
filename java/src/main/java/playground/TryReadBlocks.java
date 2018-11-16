package playground;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class TryReadBlocks {
    public static void main(String ar[])
            throws IOException {

        URL url = new URL("http://www.usbullionexchange.com/pricesonline.cfm");
        BufferedReader buf = new BufferedReader(new InputStreamReader(url.openStream()));
        String in;
        StringBuffer strbuffer = new StringBuffer();
        String temp;
        int startind = 0;
        int endind = 0;


        while ((in = buf.readLine()) != null) {
            if (in.length() > 0)
                strbuffer = strbuffer.append(" " + in);

        }

        temp = strbuffer.toString();
        startind = temp.indexOf("<form ");
        endind = temp.indexOf("</form>");

        byte[] b = temp.substring(startind, endind + 7).getBytes();
        try {
            FileOutputStream file = new FileOutputStream("somefilename.txt");
            file.write(b);
        } catch (Exception e) {
            System.out.println("Error :" + e);
        }
        buf.close();
    }

}
