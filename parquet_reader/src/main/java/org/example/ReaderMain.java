package org.example;

import java.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReaderMain {

    public static List<Object> getParquetData(String filePath) throws IOException {
        List<SimpleGroup> simpleGroups = new ArrayList<>();
        List<String> data = new ArrayList<>();
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
        ParquetMetadata footer = reader.getFooter();
        MessageType schema = footer.getFileMetaData().getSchema();
        List<Type> fields = schema.getFields();

        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            long rowCount = pages.getRowCount();
            System.out.println("Row count: " + rowCount);
            ColumnIOFactory columnIOFactory = new ColumnIOFactory();
            MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            for (int i = 0; i < rowCount; i++) {
                Group record = recordReader.read();
                if (record != null) {
                    //data.add(record.toString());
                    System.out.println("record " + i + ": " + record.toString());
                    simpleGroups.add((SimpleGroup) record);
                }
            }
        }
        reader.close();

        List<Object> result = new ArrayList<>();
        result.add(simpleGroups);
        result.add(fields);
        return result;
    }

    public static void writeDataToFile(List<String> data, String outputPath) throws IOException {
        try (FileWriter writer = new FileWriter(outputPath)) {
            for (String record : data) {
                writer.write(record + System.lineSeparator());
            }
        }
    }

    public static void main(String[] args) {
        try {
            List<Object> parquetData = getParquetData("/Users/ankit.verma/sample_parquet/Raw_6.parquet");

            List<SimpleGroup> simpleGroups = (List<SimpleGroup>) parquetData.get(0);
            int count = simpleGroups.size();

            System.out.println("Result--->");
            for (SimpleGroup group : simpleGroups) {
                System.out.println("\n " + group.toString());
            }
            System.out.println("Total records: " + count);

            List<Type> schemaFields = (List<Type>) parquetData.get(1);
            List<String> fieldNames = new ArrayList<>();
            for (Type field : schemaFields) {
                fieldNames.add(field.getName());
            }

            System.out.println("Processing complete");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}