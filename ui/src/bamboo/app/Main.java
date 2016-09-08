package bamboo.app;

import droute.Handler;
import droute.ShotgunHandler;
import droute.nanohttpd.NanoServer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;

public class Main {

    public static void main(String args[]) throws Throwable {
        if (args.length > 0 && args[0].equals("server")) {
            server(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        // we load all application classes via reflection to ensure they're loaded via JShotgun's
        // classloader when in server mode instead of in the parent classloader
        try {
            Class.forName("bamboo.app.CLI").getMethod("main", String[].class).invoke(null, (Object) args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void serverUsage() {
        System.err.println("Usage: java " + Bamboo.class.getName() + " [-b bindaddr] [-p port] [-i]");
        System.err.println("");
        System.err.println("  -b bindaddr   Bind to a particular IP address");
        System.err.println("  -i            Inherit the server socket via STDIN (for use with systemd, inetd etc)");
        System.err.println("  -p port       Local port to listen on");
    }

    public static void server(String[] args) throws IOException {
        int port = 8080;
        String host = null;
        boolean inheritSocket = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                    host = args[++i];
                    break;
                case "-i":
                    inheritSocket = true;
                    break;
                default:
                    serverUsage();
                    System.exit(1);
            }
        }
        Handler handler = new ShotgunHandler("bamboo.app.Webapp");
        if (inheritSocket) {
            Channel channel = System.inheritedChannel();
            if (channel != null && channel instanceof ServerSocketChannel) {
                new NanoServer(handler, ((ServerSocketChannel) channel).socket()).startAndJoin();
                System.exit(0);
            }
            System.err.println("When -i is given STDIN must be a ServerSocketChannel, but got " + channel);
            System.exit(1);
        }
        if (host != null) {
            new NanoServer(handler, host, port).startAndJoin();
        } else {
            new NanoServer(handler, port).startAndJoin();
        }
    }

}
