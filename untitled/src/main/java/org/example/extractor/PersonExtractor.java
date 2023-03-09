package org.example.extractor;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.SneakyThrows;
import org.example.entity.Person;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

@Component
public class PersonExtractor {
    
    public static final String FILENAME = "%s.csv";

    @SneakyThrows
    public List<Person> extractByFileName(String fileName) {
        List<Person> list = new ArrayList<>();
        FileInputStream inputStream = new FileInputStream(String.format(FILENAME, fileName));
        CsvMapper mapper = new CsvMapper();
        ObjectReader reader = mapper.reader(Person.class);
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
        MappingIterator<Person> objectMappingIterator = reader.with(csvSchema).readValues(inputStream);

        while (objectMappingIterator.hasNext()) {
            Person person = objectMappingIterator.next();
            list.add(person);
        }

        inputStream.close();
        return list;
    }

}
