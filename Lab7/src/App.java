import java.sql.*;
import oracle.jdbc.*;
import oracle.net.jdbc.TNSAddress.SOException;

public class App {

    public static void sqlCreateTable(Connection conn, String createString) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createString);
            System.out.println("Success create table");
        } catch (SQLException e) {
            System.out.println("Error create table: " + e.getMessage());
        }
    }

    public static void sqlInsertTable(Connection conn, String insertString) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(insertString);
            System.out.println("Success insert table");
        } catch (SQLException e) {
            System.out.println("Error create table: " + e.getMessage());
        }
    }

    public static void sqlDeleteRow(Connection conn, String deleteString, int id) {
        try {
            PreparedStatement pstmt = conn.prepareStatement(deleteString);
            pstmt.setInt(1, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static ResultSet sqlSelect(Connection conn, String selectString, int id) {
        try {
            PreparedStatement pstmt = conn.prepareStatement(selectString);
            pstmt.setInt(1, id);
            return pstmt.executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTables(Connection conn) {
        String createSQL = """
                    create table CLIENT (ID integer NOT NULL,
                    LAST_NAME varchar(50) NOT NULL,
                    NAME varchar(50) NOT NULL,
                    MIDDLE_NAME varchar(50) NOT NULL,
                    PASSPORT varchar(20) NOT NULL,
                    PRIMARY KEY (ID))
                """;

        sqlCreateTable(conn, createSQL);

        createSQL = """
                    create table BUS (ID integer NOT NULL,
                    CAR_NUMBER integer NOT NULL,
                    PLACES integer NOT NULL,
                    PRIMARY KEY (ID))
                """;

        sqlCreateTable(conn, createSQL);

        createSQL = """
                    create table STATION  (ID integer NOT NULL,
                    TITLE varchar(40) NOT NULL, PRIMARY KEY (ID))
                """;

        sqlCreateTable(conn, createSQL);

        createSQL = """
                    create table FLIGHT (ID integer NOT NULL,
                    TITLE varchar(40) NOT NULL,
                    PRIMARY KEY (ID))
                """;

        sqlCreateTable(conn, createSQL);

        createSQL = """
                    create table FLIGHT_STATION (
                    FLIGHT_ID integer NOT NULL,
                    STATION_ID integer NOT NULL,
                    BUSSES_ID integer NOT NULL,
                    ARRIVAL_TIME DATE NOT NULL,
                    DEPARTURE_TIME DATE NOT NULL,
                    PRIMARY KEY (FLIGHT_ID, STATION_ID),
                    FOREIGN KEY (FLIGHT_ID) REFERENCES FLIGHT (ID),
                    FOREIGN KEY (STATION_ID) REFERENCES STATION (ID),
                    FOREIGN KEY (BUSSES_ID) REFERENCES BUS (ID))
                """;

        sqlCreateTable(conn, createSQL);

        createSQL = """
                    create table TICKET (ID integer NOT NULL,
                    PRICE integer NOT NULL,
                    CLIENT_ID integer NOT NULL,
                    FLIGHT_ID integer NOT NULL,
                    PRIMARY KEY (ID),
                    FOREIGN KEY (CLIENT_ID) REFERENCES CLIENT (ID),
                    FOREIGN KEY (FLIGHT_ID) REFERENCES FLIGHT (ID))
                """;

        sqlCreateTable(conn, createSQL);
    }

    public static void insertTables(Connection conn) {
        String insertSQL = """
                    insert all
                        into CLIENT values (0, 'Hector0', 'Sharmishtha', 'Tendenov', '11 04 000000')
                        into CLIENT values (1, 'Hector1', 'Sharmishtha', 'Tendenov', '11 04 000000')
                        into CLIENT values (2, 'Hector2', 'Sharmishtha', 'Tendenov', '11 04 000000')
                        into CLIENT values (3, 'Hector3', 'Sharmishtha', 'Tendenov', '11 04 000000')
                        into CLIENT values (4, 'Hector4', 'Sharmishtha', 'Tendenov', '11 04 000000')
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);

        insertSQL = """
                    insert all
                        into BUS values (0, 111, 50)
                        into BUS values (1, 222, 30)
                        into BUS values (2, 333, 50)
                        into BUS values (3, 444, 30)
                        into BUS values (4, 555, 50)
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);

        insertSQL = """
                    insert all
                        into STATION values (0, 'A')
                        into STATION values (1, 'B')
                        into STATION values (2, 'C')
                        into STATION values (3, 'D')
                        into STATION values (4, 'E')
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);

        insertSQL = """
                    insert all
                        into FLIGHT values (0, 'FLIGHT0')
                        into FLIGHT values (1, 'FLIGHT1')
                        into FLIGHT values (2, 'FLIGHT2')
                        into FLIGHT values (3, 'FLIGHT3')
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);

        insertSQL = """
                    insert all
                        into FLIGHT_STATION values (0, 0, 0, TO_DATE('2023-05-01 10:00:00', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2023-05-02 03:00:00', 'YYYY-MM-DD HH24:MI:SS'))
                        into FLIGHT_STATION values (0, 1, 0, TO_DATE('2023-05-02 04:00:00', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2023-05-03 05:00:00', 'YYYY-MM-DD HH24:MI:SS'))
                        into FLIGHT_STATION values (0, 2, 0, TO_DATE('2023-05-03 05:00:00', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2023-05-04 07:00:00', 'YYYY-MM-DD HH24:MI:SS'))
                        into FLIGHT_STATION values (0, 3, 0, TO_DATE('2023-05-04 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('2023-05-05 03:00:00', 'YYYY-MM-DD HH24:MI:SS'))
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);

        insertSQL = """
                    insert all
                        into TICKET values (0, 1000, 0, 0)
                        into TICKET values (1, 1000, 1, 0)
                        into TICKET values (2, 1000, 2, 0)
                        into TICKET values (3, 1000, 3, 0)
                        into TICKET values (4, 1000, 4, 0)
                    select 1 from DUAL
                """;

        sqlInsertTable(conn, insertSQL);
    }

    public static void selectTables(Connection conn) {
        int flightId = 0;
        System.out.printf("Flight ID: %d\n", flightId);

        String selectSQL = """
                SELECT TITLE
                FROM STATION
                WHERE ID =
                    (SELECT STATION_ID
                    FROM FLIGHT_STATION
                    WHERE ARRIVAL_TIME = (SELECT MIN(ARRIVAL_TIME) FROM FLIGHT_STATION) AND FLIGHT_ID = ?) 
        """;
        try {
            ResultSet resultSet = sqlSelect(conn, selectSQL, flightId);
            while (resultSet.next()) {
                String title = resultSet.getString("TITLE");
                System.out.printf("Arrival station: %s\n", title);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } 
        
        selectSQL = """
            SELECT TITLE
            FROM STATION
            WHERE ID =
                (SELECT STATION_ID
                FROM FLIGHT_STATION
                WHERE DEPARTURE_TIME = (SELECT MAX(DEPARTURE_TIME) FROM FLIGHT_STATION) AND
                FLIGHT_ID = ?)
        """; 
        try {
            ResultSet resultSet = sqlSelect(conn, selectSQL, flightId);
            while (resultSet.next()) {
                String title = resultSet.getString("TITLE");
                System.out.printf("Departure station: %s\n", title);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } 
    }

    public static void deleteTables(Connection conn) {
        int flightId = 0;

        String deleteSQL = """
                    DELETE FROM TICKET WHERE FLIGHT_ID = ?
                """;

        sqlDeleteRow(conn, deleteSQL, flightId);

        deleteSQL = """
                    DELETE FROM FLIGHT_STATION WHERE FLIGHT_ID = ?
                """;

        sqlDeleteRow(conn, deleteSQL, flightId);

        deleteSQL = """
                    DELETE FROM FLIGHT WHERE ID = ?
                """;

        sqlDeleteRow(conn, deleteSQL, flightId);

    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 1521;
        String sid = "xe";
        String user = "system";
        String pwd = "123";

        DriverManager.registerDriver(new OracleDriver());
        String url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, sid);

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, pwd);
            System.out.println("Connection success");
        } catch (SQLException e) {
            System.out.println("Connection Failed : " + e.getMessage());
        }
        if (conn != null) {
            // createTables(conn);
            // insertTables(conn);
            selectTables(conn);
            // deleteTables(conn);
        }
        conn.close();
        System.out.println("Connection close");
    }
}