package orm.exception;

public class EntityIdentityNotFound extends RuntimeException {
    public EntityIdentityNotFound(String message) {
        super(message);
    }
}
