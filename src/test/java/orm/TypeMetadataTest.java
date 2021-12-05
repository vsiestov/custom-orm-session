package orm;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import orm.annotation.Column;
import orm.annotation.Entity;
import orm.annotation.Id;
import orm.annotation.Table;
import orm.metadata.TypeMetadata;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TypeMetadataTest {

    @Entity
    @Table(name = "persons")
    @Setter
    @Getter
    class Person {
        @Id(name = "person_id")
        private Long id;

        @Column(name = "first_name")
        private String firstName;

        @Column(name = "last_name")
        private String lastName;

        @Column(name = "email")
        private String email;
    }

    @Test
    void getMetaData() {
        TypeMetadata typeMetadata = new TypeMetadata(Person.class);

        assertEquals("person_id", typeMetadata.getIdentityName());
        assertEquals("persons", typeMetadata.getTableName());

        Person p = new Person();
        p.setId(123L);
        p.setEmail("vsi@container-xchange.com");
        p.setFirstName("Valerii");

        assertEquals(p.getId(), typeMetadata.getIdValue(p));

        Map<String, Object> columns = typeMetadata.getColumns(p);

        assertEquals(2, columns.size());
        assertEquals(columns.get("first_name"), p.getFirstName());
        assertEquals(columns.get("email"), p.getEmail());

        p.setLastName("Siestov");

        Map<String, Object> columns2 = typeMetadata.getColumns(p);

        assertEquals(3, columns2.size());
        assertEquals(columns2.get("first_name"), p.getFirstName());
        assertEquals(columns2.get("email"), p.getEmail());
        assertEquals(columns2.get("last_name"), p.getLastName());
    }
}
