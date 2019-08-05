package cfh.dbtester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class Main {
    
    private static final String VERSION = "1.1";
    
    private static final String DRIVERS_INI = "resources/drivers.ini";

    private static final String TABLE_FORMAT = "%-16.16s | %-16.16s | %-32.32s | %-6.6s%n";
    private static final String COLUMN_FORMAT = "%-16.16s | %-16.16s | %-12.12s | %-16.16s%n";
    
    private static LogPrinter output;
    
    public static void main(String[] args) {
        try {
            output = new LogPrinter(System.out, "dbtester.log");
            System.setOut(output);
            try {
                Main m = new Main(args);
                m.run();
            } finally {
                output.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private final List<String> drivers = new ArrayList<String>();
    private final Map<Integer, String> types = new HashMap<Integer, String>();
    
    private final String[] args;
    private boolean quiet = false;
    private final int width;
    private String url = null;
    private String user = null;
    private String pwd = null;
    private boolean tables = false;
    private List<String> columns = new ArrayList<String>();
    private String sql = null;
    private String sections = null;

    private final String SEPARATOR;
    private final String SUBSEPARATOR;
    
    private Main(String[] args) throws IOException {
        this.args = args;
        int i = 0;
        int w = -1;
        while (i < args.length && args[i].length() > 0 && args[i].startsWith("-")) {
            String arg = args[i++].substring(1).toLowerCase();
            if (arg.equals("h") || arg.equals("help")) {
                usage();
                System.exit(1);
            }
            if (arg.equals("q")) {
                quiet = true;
                output.setQuiet(true);
                continue;
            }
            if (arg.length() > 0 && Character.isDigit(arg.charAt(0))) {
                try {
                    w = Integer.decode(arg).intValue();
                    continue;
                } catch (NumberFormatException ex) {
                    exception("unrecognized option: -%s%n", arg);
                }
            }
            if (sections == null) {
                sections = arg;
            } else {
                sections += arg;
            }
        }
        width = w;
        if (i < args.length) {
            url = args[i++];
            if (url.equals("?") || url.equalsIgnoreCase("help")) {
                usage();
                System.exit(1);
            }
            if (i + 1 < args.length) {
                user = args[i++];
                pwd = args[i++];
                args[i-1] = "***";
                while (i < args.length) {
                    String option = args[i++].toLowerCase();
                    if (option.equals("tables")) {
                        tables = true;
                    } else if (option.startsWith("columns:")) {
                        String[] tokens = option.split(":", 2);
                        if (tokens.length > 1) {
                            columns.add(tokens[1]);
                        } else {
                            System.out.printf("no table given at: %s%n", option);
                        }
                    } else if (option.startsWith("sql:")) {
                        sql = option.substring(4);
                        for(;i < args.length; i++) {
                            sql += " " + args[i];
                        }
                    } else {
                        System.out.printf("unrecognized option: %s%n", option);
                    }
                }
            }
        }
        
        char[] repeat = new char[width > 0 ? width : 100];
        Arrays.fill(repeat, '=');
        SEPARATOR = new String(repeat);
        Arrays.fill(repeat, '-');
        SUBSEPARATOR = new String(repeat);
        
        readDrivers();
        initTypes();
    }
    
    private void usage() {
        System.out.println();
        System.out.println("DBTester v " + VERSION);
        System.out.println();
        System.out.println("Usage: java -jar DBTester.jar [-h] [-<width>] [-<section>...] [<url> [<user> <password> [<arguments>...]]]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("    -h            - this help");
        System.out.println("    -q            - quiet, no output, only from sql:");
        System.out.println("    -<width>      - sets output width, default 100");
        System.out.println("    -<section>... - restricts output to given section");
        System.out.println("                    c - classpath");
        System.out.println("                    d - drivers");
        System.out.println("                    l - library path");
        System.out.println("                    m - manager");
        System.out.println("                    n - network");
        System.out.println("                    p - properties");
        System.out.println("                    z - no section at all");
        System.out.println();
        System.out.println("URL:");
        System.out.println("    jdbc:<url>");
        System.out.println("    jdbc:mysql://<host>[:<port>]/<db>");
        System.out.println("    jdbc:oracle:thin:<user>/<passwd>@<host>:<port|1521>:<sid>");
        System.out.println("    jdbc:oracle:oci8:<user>/<passwd>@<host>:<port|1521>:<sid>");
        System.out.println("         ...");
        System.out.println("    ping:<host>[:<timeout>] - check if the host is reachable");
        System.out.println("    tcp:<host>:<portnumber> - open TCP");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("    tables - show list of tables");
        System.out.println("    columns:<table> - show columns of <table>");
        System.out.println("    sql:<sql> - executes SQL command");
        System.out.println();
    }
    
    private void readDrivers() throws IOException {
        InputStream in = ClassLoader.getSystemResourceAsStream(DRIVERS_INI);
        if (in == null) {
            System.out.printf("unable to open %s%n", DRIVERS_INI);
            return;
        }
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = rd.readLine()) != null) {
                if (line.trim().isEmpty() || line.trim().startsWith("#"))
                    continue;
                drivers.add(line);
            }
        } finally {
            in.close();
        }
    }
    
    private void initTypes() {
        for (Field field : Types.class.getFields()) {
            if (field.getType() == Integer.TYPE &&
                Modifier.isStatic(field.getModifiers()) &&
                Modifier.isPublic(field.getModifiers())) {
                try {
                    types.put((Integer) field.get(null), field.getName());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void run() {
        System.out.println(SEPARATOR);
        int indent = ((width > 0 ? width : 100) - 26) / 2;
        if (indent < 1) {
            indent = 1;
        }
        System.out.printf("%" + indent + "s D B T e s t e r   v %-5s%n", "", VERSION);
        System.out.printf("%" + indent + "s %2$TF  %2$TT%n", "", System.currentTimeMillis());
        System.out.println(SUBSEPARATOR);
        for (String arg : args) {
            System.out.printf("%s%n", arg);
        }
        if (runSection("n")) showNetwork();
        showDrivers(runSection("d"));
        if (runSection("m")) showDriveManager();
        if (runSection("c")) showClassPath();
        if (runSection("l")) showLibraryPath();
        if (runSection("p")) showProperties();
        if (url != null) showURL();
        System.out.println(SEPARATOR);
    }
    
    private boolean runSection(String section) {
        return sections == null || sections.contains(section);
    }
    
    private void showNetwork() {
        printHeader("NETWORK", null);
        try {
            InetAddress local = InetAddress.getLocalHost();
            boolean reachable;
            try {
                reachable = local.isReachable(500);
            } catch (IOException ex) {
                exception("%s%n", ex);
                reachable = false;
            }
            System.out.printf("Localhost: %s (%s)%n", local, reachable);
        } catch (UnknownHostException ex) {
            exception("Localhost: %s%n", ex);
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                listInterfaces("", interfaces.nextElement());
            }
        } catch (SocketException ex) {
            exception("Interfaces: %s%n", ex);
        }
    }
    
    private void listInterfaces(String indent, NetworkInterface intf) {
        System.out.printf("%sInterface: %s%n  Name: %s%n", 
            indent, intf.getName(), intf.getDisplayName());
        for (InterfaceAddress addr : intf.getInterfaceAddresses()) {
            System.out.printf("%s    %s%n", indent, addr);
        }

        StringBuilder hardware = new StringBuilder();
        try {
            byte[] hardwareAddress = intf.getHardwareAddress();
            if (hardwareAddress != null) {
                for (byte addr : hardwareAddress) {
                    if (hardware.length() > 0) {
                        hardware.append(':');
                    }
                    hardware.append(String.format("%02X", addr & 0xFF));
                }
            } else {
                hardware.append("null");
            }
        } catch (SocketException ex) {
            hardware.append(ex);
        }
        
        String mtu;
        try {
            mtu = Integer.toString(intf.getMTU());
        } catch (SocketException ex) {
            mtu = ex.toString();
        }
        
        System.out.printf("%s  Hardware: %s%n  MTU: %s%n", 
            indent, hardware, mtu);
        
        Enumeration<NetworkInterface> subInterfaces = intf.getSubInterfaces();
        while (subInterfaces.hasMoreElements()) {
            NetworkInterface sub = subInterfaces.nextElement();
            listInterfaces(indent + "  ", sub);
        }
    }
    
    private void showDrivers(boolean show) {
        if (show) {
            printHeader("DRIVERS", null);
        }
        for (String driver : drivers) {
            try {
                Class.forName(driver);
                if (show) {
                    System.out.printf("%s OK%n", driver);
                }
            } catch (ClassNotFoundException ex) {
                if (show) {
                    System.out.printf("%s%n", ex);
                }
            }
        }
    }
    
    private void showDriveManager() {
        printHeader("DRIVE MANAGER", null);
        System.out.printf("Login timeout: %d seconds%n", DriverManager.getLoginTimeout());
        Enumeration<Driver> iter = DriverManager.getDrivers();
        while (iter.hasMoreElements()) {
            Driver driver = iter.nextElement();
            String jdbc = driver.jdbcCompliant() ? "JDBC-conpliant" : "NON-compliant";
            String usable = "";
            try {
                if (url != null && driver.acceptsURL(url)) {
                    usable = "USABLE";
                }
            } catch (Exception ex) {
                usable = ex.getMessage();
            }
            System.out.printf("%-40s %2d.%-2d %14s %s%n", driver.getClass().getName(), 
                    driver.getMajorVersion(), driver.getMinorVersion(), jdbc, usable);
        }
    }
    
    private void showClassPath() {
        printHeader("CLASSPATH", null);
        String[] paths = System.getProperty("java.class.path").split(";");
        for (String path : paths) {
            System.out.printf("%s%n", path);
        }
    }
    
    private void showLibraryPath() {
        printHeader("LIBRARIES", null);
        String[] paths = System.getProperty("java.library.path").split(";");
        for (String path : paths) {
            System.out.printf("%s%n", path);
        }
    }

    private void showProperties() {
        printHeader("SYSTEM PROPERTIES", null);
        Properties props = System.getProperties();
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            System.out.printf("%s = %s%n", key, value);
        }
    }
    
    private void showURL() {
        if (url.startsWith("ping:")) {
            printPing(url.substring(5));
        } else if (url.startsWith("tcp:")) {
            printTCP(url.substring(4));
        } else {
            printConnect();
        }
    }
    
    private void printPing(String address) {
        printHeader("PING", address);
        String host = "127.0.0.1";
        String[] tokens = address.split(":",2);
        if (tokens.length >= 1 && !tokens[0].isEmpty()) {
            host = tokens[0];
        }
        int timeout = 3000;
        if (tokens.length >= 2 && !tokens[1].isEmpty()) {
            try {
                timeout = Integer.parseInt(tokens[1]);
            } catch (NumberFormatException ex) {
                exception("Timeout: %s%n", ex);
                return;
            }
        }
        
        InetAddress inet;
        try {
            inet = InetAddress.getByName(host);
        } catch (UnknownHostException ex) {
            exception("Host: %s%n", ex);
            return;
        }
        try {
            boolean reachable = inet.isReachable(timeout);
            System.out.printf("%s (%s): %s%n", inet, host, reachable ? "OK" : "unreachable");
        } catch (IOException ex) {
            exception("%s (%s): %s%n", inet, host, ex);
        }
    }
    
    private void printTCP(String address) {
        printHeader("TCP", address);
        String[] tokens = address.split(":", 3);
        if (tokens.length < 2) {
            System.out.printf("Address: missing port%n");
            return;
        }
        String host = tokens[0];
        if (host.isEmpty()) {
            host = "localhost";
        }
        int port;
        try {
            port = Integer.parseInt(tokens[1]);
        } catch (NumberFormatException ex) {
            exception("Port: %s%n", ex);
            return;
        }
        int timeout = 1000;
        if (tokens.length >= 3) {
            try {
                timeout = Integer.parseInt(tokens[2]);
            } catch (NumberFormatException ex) {
                exception("Timeout: %s%n", ex);
                return;
            }
        }
        
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), timeout);
            socket.setSoTimeout(timeout);
            byte[] buff = new byte[256];
            int read = 0;
            try {
                read = socket.getInputStream().read(buff);
                System.out.printf("Read: %d bytes%n%s%n%s%n", 
                    read, Arrays.toString(buff), new String(buff, 0, read));
            } catch (IOException ex) {
                exception("%s%n", ex);
            }
            socket.close();
        } catch (IOException ex) {
            exception("Tcp: %s%n", ex);
        }
    }

    private void printConnect() {
        printHeader("CONNECT", url + " " + user);
        try {
            Connection conn;
            if (user == null || user.equals("-")) {
                conn = DriverManager.getConnection(url);
            } else {
                conn = DriverManager.getConnection(url, user, pwd);
            }
            try {
                DatabaseMetaData metaData = conn.getMetaData();
                try {
                    String name = metaData.getDatabaseProductName();
                    System.out.printf("Product: %s%n", name);
                } catch (SQLException ex) {
                    exception("Product: %s%n", ex);
                }
                try {
                    int major = metaData.getDatabaseMajorVersion();
                    System.out.printf("Major: %d%n", major);
                } catch (SQLException ex) {
                    exception("Major: %s%n", ex);
                }
                try {
                    int minor = metaData.getDatabaseMinorVersion();
                    System.out.printf("Minor: %d%n", minor);
                } catch (SQLException ex) {
                    exception("Minor: %s%n", ex);
                }
                try {
                    System.out.printf("Catalogs:%n");
                    ResultSet rset = metaData.getCatalogs();
                    while (rset.next()) {
                        System.out.printf("    %s%n", rset.getString(1));
                    }
                    rset.close();
                } catch (SQLException ex) {
                    exception("    %s%n", ex);
                }
                if (tables) {
                    listTables(metaData);
                }
                for (String table : columns) {
                    listColumns(metaData, table);
                }
                if (sql != null) {
                    executeSQL(conn);
                }
            } catch (SQLException ex) {
                exception("MetaData: %s%n", ex);
            } finally {
                conn.close();
            }
        } catch (SQLException ex) {
            exception("Connection: %s%n", ex);
        }
    }

    private void listTables(DatabaseMetaData metaData) {
        printHeader("TABLES", null);
        try {
            ResultSet rset = metaData.getTables(null, null, "%", null);
            boolean first = true;
            while (rset.next()) {
                if (first) {
                    System.out.printf(TABLE_FORMAT, "CATALOG", "SCHEMA", "NAME", "TYPE");
                }
                first = false;
                System.out.printf(TABLE_FORMAT, 
                        rset.getString("TABLE_CAT"), 
                        rset.getString("TABLE_SCHEM"), 
                        rset.getString("TABLE_NAME"), 
                        rset.getString("TABLE_TYPE"));
            }
            if (first) {
                System.out.printf("NO TABLE FOUND%n");
            }
        } catch (SQLException ex) {
            exception("Tables: %s%n", ex);
        }
    }

    private void listColumns(DatabaseMetaData metaData, String table) {
        printHeader("COLUMNS ", table);
        try {
            ResultSet tab = metaData.getTables(null, null, "%", null);
            boolean firstTable = true;
            while (tab.next()) {
                String name = tab.getString("TABLE_NAME");
                if (name.equalsIgnoreCase(table)) {
                    if (!firstTable) {
                        System.out.println(SUBSEPARATOR);
                    }
                    firstTable = false;
                    String catalog = tab.getString("TABLE_CAT");
                    String schema = tab.getString("TABLE_SCHEM");
                    System.out.printf("Table: %s, Catalog: %s, Schema: %s%n", name, catalog, schema);
                    ResultSet rset = metaData.getColumns(catalog, schema, name, "%");
                    boolean first = true;
                    while (rset.next()) {
                        if (first) {
                            System.out.printf(COLUMN_FORMAT, "NAME", "TYPE", "SIZE", "DEFAULT");
                        }
                        first = false;
                        String sizeText;
                        int size = rset.getInt("COLUMN_SIZE");
                        if (rset.wasNull()) {
                            sizeText = "";
                        } else {
                            sizeText = Integer.toString(size);
                            int decimal = rset.getInt("DECIMAL_DIGITS");
                            if (!rset.wasNull()) {
                                sizeText += "." + Integer.toString(decimal);
                            }
                        }
                        System.out.printf(COLUMN_FORMAT,
                                rset.getString("COLUMN_NAME"),
                                rset.getString("TYPE_NAME"),
                                sizeText,
                                rset.getString("COLUMN_DEF"));
                    }
                }
            }
            if (firstTable) {
                System.out.printf("NO TABLE FOUND%n");
            }
        } catch (SQLException ex) {
            exception("Columns: %s%n", ex);
        }
    }

    private void executeSQL(Connection conn) {
        printHeader("SQL", sql);
        try {
            Statement stmt = conn.createStatement();
            try {
                boolean isResultSet = stmt.execute(sql);
                int count = stmt.getUpdateCount();
                do {
                    if (isResultSet) {
                        listResultSet(stmt.getResultSet());
                    } else {
                        System.out.printf("count: %d%n", count);
                    }
                    isResultSet = stmt.getMoreResults();
                    count = stmt.getUpdateCount();
                    if (isResultSet || count != -1) {
                        System.out.printf("%s%n", SUBSEPARATOR);
                    }
                } while (isResultSet || count != -1);
            } catch (SQLException ex) {
                exception("Execute: %s%n", ex);
            } finally {
                stmt.close();
            }
        } catch (SQLException ex) {
            exception("Create: %s%n", ex);
        }
    }
    
    private void listResultSet(ResultSet rset) {
        String format;
        
        format = "%-12.12s|%-12.12s|%-15.15s|%-15.15s|%-12.12s|%4.4s.%-4.4s|%-10.10s%n";
        System.out.printf(format, "Catalog", "Schema", "Table", "Name", "TypeName", "Precision", "Scale","JDBCType");
        System.out.printf(format, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR, SUBSEPARATOR);
        try {
            ResultSetMetaData meta = rset.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                System.out.printf(format, 
                    meta.getCatalogName(i),
                    meta.getSchemaName(i),
                    meta.getTableName(i),
                    meta.getColumnName(i), 
                    meta.getColumnTypeName(i), 
                    meta.getPrecision(i), 
                    meta.getScale(i),
                    typeToString(meta.getColumnType(i)));
            }
            
            System.out.println(SUBSEPARATOR);
            Object[] labels = new String[count];
            int[] sizes = new int[count];
            int total = -1;
            for (int i = 0; i < count; i++) {
                labels[i] = meta.getColumnLabel(i+1);
                sizes[i] = meta.getColumnDisplaySize(i+1);
                total += sizes[i] + 1;
            }
            int max = width > 0 ? width : 100;
            if (total > max) {
                int mean = 2 * ((max+count-1) / count);
                if (mean > 15) {
                    while (total > max) {
                        boolean changed = false;
                        for (int i = count-1; i >= 0; i--) {
                            if (sizes[i] > mean) {
                                changed = true;
                                total -= 1;
                                sizes[i] -= 1;
                                if (total <= 80)
                                    break;
                            }
                        }
                        if (!changed) {
                            mean = (int) (mean * 0.7);
                            if (mean < 7)
                                break;
                        }
                    }
                } else {
                    for (int i = 0; i < count; i++) {
                        if (sizes[i] > 20) {
                            total -= sizes[i] - 20;
                            sizes[i] = 20;
                        }
                    }
                }
            }
            StringBuilder builder = new StringBuilder();
            for (int size : sizes) {
                if (builder.length() > 0) {
                    builder.append("|");
                }
                builder.append("%-").append(size).append(".").append(size).append("s");
            }
            builder.append("%n");
            format = builder.toString();
            
            Object[] values = new Object[count];
            Arrays.fill(values, SUBSEPARATOR);
            System.out.printf(format, labels);
            System.out.printf(format, values);
            try {
                output.setQuiet(false);
                while (rset.next()) {
                    for (int i = 0; i < count; i++) {
                        values[i] = rset.getObject(i+1);
                    }
                    System.out.printf(format, values);
                }
            } finally {
                output.setQuiet(quiet);
            }
        } catch (SQLException ex) {
            exception("RS-Meta: %s%n", ex);
            return;
        }
    }

    private String typeToString(int type) {
        return types.get(type);
    }

    private void printHeader(String header, String subheader) {
        StringBuilder builder = new StringBuilder(header);
        for (int i = builder.length(); i >= 0; i--) {
            builder.insert(i, ' ');
        }
        if (subheader != null) {
            builder.append("  ").append(subheader);
        }
        System.out.println(SEPARATOR);
        System.out.printf(" %s%n", builder);
        System.out.println(SUBSEPARATOR);
    }
    
    private void exception(String format, Object... args) {
        try {
            output.setQuiet(false);
            System.out.printf(format, args);
        } finally {
            output.setQuiet(quiet);
        }
    }
}
