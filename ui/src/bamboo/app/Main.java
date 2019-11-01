package bamboo.app;

import bamboo.Application;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class Main {

    public static void main(String args[]) throws Throwable {
        if (args.length > 0 && args[0].equals("server")) {
            Application.main(Arrays.copyOfRange(args, 1, args.length));
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
}