package bamboo.app;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.ThreadPool;
import spark.Spark;
import spark.embeddedserver.EmbeddedServer;
import spark.embeddedserver.EmbeddedServers;
import spark.embeddedserver.jetty.EmbeddedJettyFactory;
import spark.embeddedserver.jetty.EmbeddedJettyServer;
import spark.embeddedserver.jetty.JettyHandler;
import spark.embeddedserver.jetty.JettyServerFactory;
import spark.http.matching.MatcherFilter;
import spark.route.Routes;
import spark.staticfiles.StaticFilesConfiguration;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
        System.err.println("  -p port       Local port to listen on");
        System.err.println("  -c path       Set context path");
    }

    public static void server(String[] args) throws IOException, InterruptedException {
        int port = 8080;
        String host = null;
        String contextPath = "";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                    host = args[++i];
                    break;
                case "-c":
                    contextPath = args[++i];
                    break;
                default:
                    serverUsage();
                    System.exit(1);
            }
        }
        Spark.port(port);
        if (host != null) {
            Spark.ipAddress(host);
        }

        {
            String path = contextPath.replaceFirst("/+$", "");
            EmbeddedServers.add(EmbeddedServers.Identifiers.JETTY, new EmbeddedJettyFactory() {
                @Override
                public EmbeddedServer create(Routes routeMatcher, StaticFilesConfiguration staticFilesConfiguration, boolean hasMultipleHandler) {
                    MatcherFilter matcherFilter = new MatcherFilter(routeMatcher, staticFilesConfiguration, false, hasMultipleHandler);
                    matcherFilter.init((FilterConfig) null);
                    JettyHandler handler = new JettyHandler(matcherFilter) {
                        @Override
                        public void doHandle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
                            request = new HttpServletRequestWrapper(request) {
                                @Override
                                public String getRequestURI() {
                                    // Spark unfortunately uses getRequestURI() for routing rather than pathInfo
                                    // so we work around it here.
                                    return super.getRequestURI().substring(path.length());
                                }
                            };
                            super.doHandle(target, baseRequest, request, response);
                        }
                    };
                    ServletContextHandler servletContextHandler = new ServletContextHandler();
                    servletContextHandler.setContextPath(path);
                    servletContextHandler.setHandler(handler);

                    HandlerWrapper wrapper = new HandlerWrapper() {
                        @Override
                        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
                            if (target.equals("/")) {
                                response.sendRedirect(path);
                            } else {
                                super.handle(target, baseRequest, request, response);
                            }
                        }
                    };
                    wrapper.setHandler(servletContextHandler);

                    return new EmbeddedJettyServer(new JettyServerFactory() {
                        @Override
                        public Server create(int i, int i1, int i2) {
                            return new Server();
                        }

                        @Override
                        public Server create(ThreadPool threadPool) {
                            return new Server(threadPool);
                        }
                    }, path.isEmpty() ? servletContextHandler : wrapper);
                }
            });
        }

        try (Webapp webapp = new Webapp()) {
            Spark.awaitInitialization();
            while (true) {
                Thread.sleep(1000000000);
            }
        }
    }
}