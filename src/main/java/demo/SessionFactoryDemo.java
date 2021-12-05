package demo;

import lombok.SneakyThrows;
import org.postgresql.ds.PGSimpleDataSource;
import orm.Session;
import orm.SessionFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SessionFactoryDemo {
    @SneakyThrows
    public static void main(String[] args) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setURL("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        dataSource.setCurrentSchema("bobocode");

        SessionFactory sessionFactory = new SessionFactory(dataSource);
        Session session = sessionFactory.createSession();

        Person newPerson = new Person();

        newPerson.setFirstName("New Person First Name");
        newPerson.setLastName("New Person Last Name");
        newPerson.setEmail("vsi@container-xchange.com");

        session.persist(newPerson);

        List<Person> list = session.find(Person.class);

        session.remove(list.get(list.size() - 1));

        byte[] array = new byte[7];
        new Random().nextBytes(array);
        Person secondPerson = session.find(Person.class, 2L);
        secondPerson.setLastName(new String(array, UTF_8));

        session.close();
    }
}
