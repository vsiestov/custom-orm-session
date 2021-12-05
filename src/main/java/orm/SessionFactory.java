package orm;

import lombok.RequiredArgsConstructor;
import javax.sql.DataSource;
import java.sql.SQLException;

@RequiredArgsConstructor
public class SessionFactory {
    private final DataSource dataSource;

    public Session createSession() throws SQLException {
        return new Session(dataSource);
    }
}
