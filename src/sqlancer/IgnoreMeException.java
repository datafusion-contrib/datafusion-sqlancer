package sqlancer;

public class IgnoreMeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IgnoreMeException() {
        super();
    }

    public IgnoreMeException(String message) {
        super(message);
    }
}
