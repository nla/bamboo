package bamboo.task;

public class Task {
    final Runnable runnable;
    final String name;
    Thread thread;
    boolean enabled = true;

    public Task(Runnable runnable) {
        this.runnable = runnable;
        name = runnable.getClass().getSimpleName().replaceAll("([a-z])([A-Z])", "$1 $2");
    }

    public String getStatus() {
        if (!enabled) {
            if (isRunning()) {
                return "Stopping";
            } else {
                return "Disabled";
            }
        }
        if (!isRunning()) {
            return "Idle";
        }
        return "Running";
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return runnable.getClass().getSimpleName();
    }

    public void disable() {
        enabled = false;
        if (isRunning()) {
            thread.interrupt();
        }
    }

    public void enable() {
        enabled = true;
        start();
    }

    public boolean isRunning() {
        return thread != null && thread.isAlive();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void start() {
        if (enabled && !isRunning()) {
            thread = new Thread(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            thread.start();
        }
    }
}
