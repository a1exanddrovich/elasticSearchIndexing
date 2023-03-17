package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class Person {
    private String id;
    private int age;
    private String name;
    private String department;
    private String geoLocation;

}
