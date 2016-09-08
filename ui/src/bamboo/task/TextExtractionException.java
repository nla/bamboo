package bamboo.task;

public class TextExtractionException extends Exception {
    public TextExtractionException() {
        super();
    }

    public TextExtractionException(String s) {
        super(s);
    }

    public TextExtractionException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public TextExtractionException(Throwable e) {
        super(e);
    }

    protected TextExtractionException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}
