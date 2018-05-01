package db;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {

    // Various common constructs, simplifies parsing.
    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD   = Pattern.compile("load " + REST),
            STORE_CMD  = Pattern.compile("store " + REST),
            DROP_CMD   = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD  = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*"
            + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                    + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                    + "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+"
                    + "[\\w\\s+\\-*/'<>=!]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+"
                    + SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?"
                    + "\\s*(?:,\\s*.+?\\s*)*)");

    private static final Pattern VALID_CONDS = Pattern.compile("(\\S+)\\s*"
            + "((?:==)|(?:!=)|(?:<)|(?:>)|(?:<=)|(?:>=))"
            + "\\s*(\\S+)");

    //member variables
    private HashMap<String, Table> tables;
    private int size;

    public Database() {
        // YOUR CODE HERE
        size = 0;
        tables = new HashMap<String, Table>();
    }

    public Table get(String name) {
        return tables.get(name);
    }

    public void add(String name, Table t) {
        tables.put(name, t);
    }

    public String transact(String query) {
        String[] q = new String[1];
        q[0] = query;

        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertRow(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            return String.format("ERROR: Malformed query: %s\n", query);
        }
    }

    private String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return String.format("ERROR: Malformed create: %s\n", expr);
        }
    }

    private String createNewTable(String name, String[] cols) {
        if (get(name) != null) {
            return String.format("ERROR: table %s already exists\n", name);
        } else if (cols == null) {
            return "ERROR: can't make table with no columns\n";
        } else if (!goodColumn(cols)) {
            return "ERROR: can't make table with bad columns\n";
        }
        add(name, new Table(name, cols));
        size++;
        return "";
    }

    private String createSelectedTable(String name, String exprs, String tbls, String conds) {
        if (get(name) != null) {
            return String.format("ERROR: table %s already exists\n", name);
        }
        try {
            Table result = select(exprs, tbls, conds);
            result.rename(name);
            add(name, result);
            return "";
        } catch (QueryException e) {
            return e.getMessage();
        }
    }

    //returns true if the column is of the right format to build a table.
    public boolean goodColumn(String[] cols) {
        for (int i = 0; i < cols.length; i++) {
            Pattern valid = Pattern.compile("\\s*(\\w+)\\s+(\\w+)\\s*");
            Matcher m = valid.matcher(cols[i]);
            String[] member = cols[i].split("\\s+");

            if (!m.matches()) {
                return false;
            }

            if (!m.group(2).equalsIgnoreCase("int")
                && !m.group(2).equalsIgnoreCase("float")
                && !m.group(2).equalsIgnoreCase("string")) {
                return false;
            }
        }
        return true;
    }

    private String loadTable(String name) {
        name = name + ".tbl";
        BufferedReader reader = null;
        Table loaded = null;
        String[] data;
        String[] cols;
        String tableName = name.split("\\.")[0];
        String msg = "";

        try {
            File file = new File(name);
            reader = new BufferedReader(new FileReader(file));

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                if (count == 0) {
                    cols = line.split(COMMA);
                    if (goodColumn(cols)) {
                        loaded = new Table(tableName, cols);
                        count++;
                    } else {
                        return "ERROR: File not found exception"; //QueryException();
                    }
                } else {
                    data = line.split(COMMA);
                    try {
                        msg = loaded.insertRow(data);
                    } catch (QueryException e) {
                        reader.close();
                        return e.getMessage(); //insert row
                    } catch (NullPointerException e2) {
                        reader.close();
                        return e2.getMessage();
                    }
                }
            }
            this.add(loaded.name(), loaded);
            reader.close();

        } catch (IOException e) {
            //reader.close();
            return "ERROR: File not found exception";
            //e.printStackTrace();
        } catch (NullPointerException e2) {
            return "ERROR: File has an error: ";
        }
        return msg;
    }

    private String storeTable(String name) {
        if (!this.tables.containsKey(name)) {
            return "ERROR: Database does not contain table";
        }

        name = name + ".tbl";
        PrintWriter writer = null;
        String tableName = name.split("\\.")[0];
        Table loaded = this.get(tableName);
        String msg = "";

        try {
            File file = new File(name);
            writer = new PrintWriter(new FileWriter(file, false));
            String tableStr = loaded.print();
            writer.print(tableStr);
            writer.close();

        } catch (IOException e) {
            return "ERROR: File not found exception";
        } catch (NullPointerException e) {
            return "ERROR: File not found exception";
        }

        return msg;
    }

    private String dropTable(String name) {
        //System.out.printf("You are trying to drop the table named %s\n", name);
        if (this.tables.containsKey(name)) {
            this.tables.remove(name);
            return "";
        }
        return "ERROR: Database does not contain " + name + ".tbl";
    }

    private String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);

        if (!m.matches()) {
            return String.format("ERROR: Malformed insert: %s\n", expr);
        }

        String tableName = m.group(1);

        if (!this.tables.containsKey(tableName)) {
            return String.format("ERROR: Malformed insert: %s\n", expr);
        }
        String rowData = m.group(2);

        String [] rowMembers = rowData.split(",");
        try {
            return this.get(tableName).insertRow(rowMembers);
        } catch (QueryException e) {
            return e.getMessage();
        }
    }

    private String printTable(String name) {
        Table t = get(name);
        return (t == null) ? "ERROR: table doesn't exist\n" : t.print();
    }

    private String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return String.format("ERROR: Malformed select: %s\n", expr);
        }
        try {
            return select(m.group(1), m.group(2), m.group(3)).print();
        } catch (QueryException e) {
            return e.getMessage();
        }
    }

    private Table select(String exprs, String tbls, String conds) throws QueryException {
        Supplier<Stream<String>> sm = () -> Stream.of(tbls.split(COMMA));
        String n;
        n = sm.get().filter(tn -> !this.tables.containsKey(tn)).collect(Collectors.joining(", "));
        if (n.length() != 0) {
            throw new QueryException(String.format("ERROR: table(s)%s don't exist\n", n));
        }
        Supplier<Stream<Table>> tSm = () -> sm.get().map(this::get);
        Table first = tSm.get().findFirst().isPresent() ? tSm.get().findFirst().get() : null;
        if (first == null) {
            throw new QueryException("ERROR: no tables selected from\n");
        }
        Table joined = tSm.get().reduce(first, Table::join);
        joined = joined.selectExpr(exprs);
        if (conds != null) {
            String[] conditions = conds.split(AND);
            Matcher m;
            for (String condition : conditions) {
                if ((m = VALID_CONDS.matcher(condition)).matches()) {
                    joined = joined.selectRows(m.group(1), m.group(2), m.group(3));
                    continue;
                }
                throw new QueryException(String.format("ERROR: invalid cond: %s\n", condition));
            }
        }
        return joined;
    }

}
