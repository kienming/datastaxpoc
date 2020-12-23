package org.km.poc.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Properties;
import java.util.List;
import java.util.UUID;


public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        if(args == null || args.length < 1) {
            throw new IllegalArgumentException("Properties path should be defined");
        }

        log.debug("Starting application");
        Application app = new Application();

        String propertiesPath = args[0];
        Properties p = app.readPropertiesFile(propertiesPath);

        log.debug("Creating datastax properties");
        DataStaxProperties dsp = new DataStaxProperties(p);

        app.runDatastax(dsp);

        System.exit(0);
    }

    private Properties readPropertiesFile(String path) {
        log.debug("Start reading properties file");
        File f = new File(path);
        if(!f.exists()) {
            throw new IllegalArgumentException("Properties file given not found");
        }

        if(!f.isFile()) {
            throw new IllegalArgumentException("Properties file path given is not a file");
        }

        Properties p = new Properties();

        try (InputStream in = new FileInputStream(f)) {
            p.load(in);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Properties file not found", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read properties file provided", e);
        }

        return p;
    }

    private void runDatastax(DataStaxProperties p) {
        log.debug("Start running datastax");
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(p.getSecureConnectBundlePath()))
                .withAuthCredentials(p.getUsername(),p.getPassword())
                .withKeyspace(p.getKeyspaceName())
                .build()) {
            executeGet(session);
            executeInsert(session, p.getInsertCount());
        } catch (Exception e) {
            throw new RuntimeException("Failed to run datastax", e);
        }
    }

    private void executeGet(CqlSession session) throws Exception {
        log.debug("Execute select query.....");
        ResultSet rs = session.execute("SELECT * FROM VIDEOS");

        List<Row> rowList = rs.all();

        if(rowList.isEmpty()) {
            log.debug("Row returned is empty");
            return;
        }

        VideoMapper mapper = new VideoMapper();
        for(int i = 0; i < rowList.size(); i++) {
            Row row = rowList.get(i);
            Video vd = mapper.map(row);
            log.debug("Records #{}: {}", i + 1, vd);

        }
    }

    private void executeInsert(CqlSession session, int insertCount) throws Exception {
        log.debug("Execute insert query..... inserting {} rows", insertCount);
        PreparedStatement ps = session.prepare("insert into videos (video_id,added_date,title) values (?, ?, ?)");
        for(int i = 0; i < insertCount; i++) {
            UUID uuid = Uuids.timeBased();
            log.debug("Inserting data #{} Generating uuid. uuid: {}", i + 1, uuid);
            BoundStatement bound = ps.bind(uuid, Instant.ofEpochMilli(System.currentTimeMillis()), MessageFormat.format("Title #{0}", i + 1));
            session.execute(bound);
        }
    }
}
