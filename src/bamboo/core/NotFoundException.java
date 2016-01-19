package bamboo.core;

public class NotFoundException extends RuntimeException {

    public NotFoundException() {
        super();
    }

    public NotFoundException(String s) {
        super(s);
    }

    public NotFoundException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public NotFoundException(Throwable throwable) {
        super(throwable);
    }

    protected NotFoundException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }

    public NotFoundException(String type, long id) {
        this(type + " " + id);
    }


    public static <T> T check(T object, String type, long id) {
        if (object == null) {
            throw new NotFoundException(type, id);
        }
        return object;
    }
}
