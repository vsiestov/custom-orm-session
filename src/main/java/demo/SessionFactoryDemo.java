package demo;

import org.postgresql.ds.PGSimpleDataSource;
import orm.Session;
import orm.SessionFactory;

import java.util.List;

public class SessionFactoryDemo {
    public static void main(String[] args) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setURL("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        dataSource.setCurrentSchema("bobocode");

        SessionFactory sessionFactory = new SessionFactory(dataSource);
        Session session = sessionFactory.createSession();

        List<Person> people = session.find(Person.class);

        System.out.println(people);

        Person person2 = session.find(Person.class, 2L);

        System.out.println(person2);
        System.out.println(session.find(Person.class, 2L));
        System.out.println(session.find(Person.class, 1L));
        System.out.println(session.find(Person.class, 2L));

        person2.setLastName("Changed");

        session.close();
    }
}
