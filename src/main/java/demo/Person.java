package demo;

import lombok.Data;
import lombok.ToString;
import orm.annotation.Column;
import orm.annotation.Entity;
import orm.annotation.Id;
import orm.annotation.Table;

@Entity
@Table(name = "persons")
@Data
@ToString
public class Person {
    @Id
    private Long id;

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "last_name")
    private String lastName;
}
