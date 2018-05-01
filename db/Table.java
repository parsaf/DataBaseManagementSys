package db;


import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.*;
import java.util.stream.Collector;
import java.util.Arrays;

/**
 * Created by BryceSchmidtchen on 2/25/17.
 */

public class Table {
    private static String NAN = "NaN",
            NOVALUE = "NOVALUE";

    private class Column {
        private ArrayList<Object> values;
        private String column_name;
        private String type;

        Column(String name, String type_str) {
            this.values = new ArrayList<>();
            this.type = type_str;
            this.column_name = name;
        }

        public String name() {
            return column_name;
        }

        public String type() {
            return type;
        }

        public ArrayList<Object> values() {
            return values;
        }

        public Object getItem(int i) {
            return values.get(i);
        }

        private Object getItemPrint(int i) {
            Object val = values.get(i);
            if (this.type().equalsIgnoreCase("float") && ! val.equals(NAN) && ! val.equals(NOVALUE)) {
                val = String.format("%.3f", (Float)val);
            }
            return val;
        }

        public int size() {
            return values.size();
        }

        public Column insertItem(Object item) {
            values.add(item);
            return this;
        }

        public Column SetCopy(Object[] rows){
            Column newCol = new Column(this.name(), this.type());

            for(int i = 0; i < rows.length; i++){
                newCol.insertItem(this.values().get((Integer) rows[i]));
            }

            return newCol;
        }

        public Column rowMult(int times_per_row){
            Column newCol = new Column(this.name(), this.type());

            for(int x = 0; x < this.size(); x++){
                for(int i = 0; i < times_per_row; i++){
                    //newCol.values.addAll(this.values());
                    newCol.values.add(this.getItem(x));
                }
            }
            return newCol; //should return the current column with all elements now repeated times_per_row times
        }

        public Column valuesMult(int times_per_list){
            //should return the current column with the values repeated times_per_list times
            Column newCol = new Column(this.name(), this.type());

            for(int i = 0; i < times_per_list; i++){
                newCol.values.addAll(this.values());
            }

            return newCol;
        }
    }

    private String table_name;
    private ArrayList<Column> columns;
    private HashMap<String, Integer> col_indices;
    private String[] ctorCols;

    // TODO: 3/2/2017 update numberOfRows in appropriate Ctors and methods

    private int numberOfRows;

    Table(String name, String[] cols) {
        this.col_indices = new HashMap<>();
        this.table_name = name;
        this.ctorCols = cols;
        this.columns = new ArrayList<>();
        this.numberOfRows = 0;
        if (cols != null) {
            for (int i = 0; i < cols.length; i++) {
                String[] temp = cols[i].split("\\s+");
                col_indices.put(temp[0], i);
                columns.add(new Column(temp[0], temp[1]));
            }
        }
    }

    public void rename(String name) {
        this.table_name = name;
    }

    public void ColumnSet(Column newCol){
        //given a column that we KNOW has its name in our table, set that column with the same name to this column.
        int index = this.col_indices.get(newCol.name());
        this.columns.set(index, newCol);
        //now this table is modified with the new column.
    }

    public ArrayList<String> columnNameAndTypeList(){
        return new ArrayList<String>(Arrays.asList(this.ctorCols));

    }

    public ArrayList<String> columnNameList(){
        //return new ArrayList<String>(Arrays.asList(this.ctorCols));
        //go through columns and return an array list of the names ONLY.
        ArrayList<String> columnsNames = new ArrayList<>();
        for(int i = 0; i < this.columns.size(); i++){
            columnsNames.add(this.columns.get(i).name());
        }
        return columnsNames;
    }

    public int IndexFinder(String colName) {
        return this.col_indices.get(colName);
    }

    public void numRowsSetter(int row_num) {
        this.numberOfRows = row_num;
    }


    public Set tableSet() {
        return this.col_indices.keySet();
    }

    public String titleStringRow(){
        String rowData = "";
        for(int i = 0; i < ctorCols.length - 1; i++){
            rowData = rowData + ctorCols[i] + ",";
        }
        rowData = rowData + ctorCols[ctorCols.length - 1];
        return rowData;
    }

    public String dataStringRow(int rowNumber){
        String rowData = "";
        if(rowNumber > numberOfRows){
            return "Error: there aren't that many rows in this table, BUDDY";
        }
        for (int i = 0; i < this.columns.size() - 1; i++){
            rowData = rowData + this.columns.get(i).getItem(rowNumber).toString() + ",";
        }
        rowData = rowData + this.columns.get(this.columns.size() - 1).getItem(rowNumber).toString();
        return rowData;
    }

    public String insertRow(String[] members) throws QueryException{


        String msg = "";
        if (members.length != this.columns.size()) {
            //System.out.print("hi");
            return "ERROR: Row does not match table";
        }

        for (int i = 0; i < this.columns.size(); i++) {
            //columns.get(i).insert_item(members[i]);

            String type = columns.get(i).type();
            String member = members[i];

            int intVal = 0;
            float floatVal = 0;
            String strVal = "";

            if(!type.equalsIgnoreCase("int") && !type.equalsIgnoreCase("float") && !type.equalsIgnoreCase("string")){
                return "ERROR: Columns type ";
            }

            if(member.equals(NOVALUE) || member.equals(NAN)){
                Object no_val = member;
                Column curr = columns.get(i);
                curr.insertItem(no_val);
                columns.set(i, curr);
            }
            else if (type.equalsIgnoreCase("int")) {
                try {
                    intVal = Integer.parseInt(member);
                } catch (NumberFormatException e1) {
                    throw new QueryException("ERROR: type incompatible");
                    //return "ERROR: Row does not match table";
                }
                Column curr = columns.get(i);
                curr.insertItem(intVal);
                columns.set(i, curr);
            } else if (type.equalsIgnoreCase("float")) {
                try {
                    floatVal = Float.parseFloat(member);
                } catch (NumberFormatException e1) {
                    throw new QueryException("ERROR: type incompatible");
                }

                Column curr = columns.get(i);
                curr.insertItem(floatVal);
                columns.set(i, curr);
            } else {
                if (!member.startsWith("'") || !member.endsWith("'")) {
                    return "ERROR: Malformed data entry: " + member;
                }
                strVal = member;
                Column curr = columns.get(i);
                curr.insertItem(strVal);
                columns.set(i, curr);
            }

        }
        numberOfRows += 1;
        return msg;
    }


    public String name() {
        return table_name;
    }

    public int num_rows(){
        return this.numberOfRows;
    }

    private void setCtorCols(){
        ctorCols = columns.stream().map(c -> c.name() + " " + c.type()).collect(Collectors.joining(",")).split(",");
    }

    public String print() {
        // string of comma separated pairs of name and type.

        //how we can convert to decimal place for float

        String result = columns.stream().map(c -> c.name() + " " + c.type()).collect(Collectors.joining(","));

        for (int i = 0; i < numberOfRows; i++) {
            final int n = i;
            // string representation of the row with values comma separated
            String row = columns.stream().map(c -> c.getItemPrint(n).toString()).collect(Collectors.joining(","));
            result += "\n" + row;
        }
        return result;
    }

    public static class Jtest {
        @Test
        public void TestIt() {
            //System.out.print(" b string".split("\\s+").length);

            //select TeamName,Sport,Season,Wins,Losses from teamRecords where TeamName > Sport and Wins > Losses
            Database db = new Database();
            String result = db.transact("load examples/records");
            String res = db.transact("select TeamName,Season,Wins,Losses from examples/records where Wins >= Losses");
            System.out.print(res);

            Table a = new Table("t", new String[]{"x int", "y int"});
            a.columns.get(0).values.add(2000000000);
            a.columns.get(0).values.add(2147483647);
            a.columns.get(0).values.add(1);

            a.columns.get(0).values.add(147483577);
            a.columns.get(1).values.add(104);
            a.columns.get(1).values.add(2147483556);
            a.numberOfRows = 3;

            db.add("t", a);

            //String res = db.transact("select x + y as z from t");
            //System.out.print(res);
        }
    }

    public static void main(){
        jh61b.junit.TestRunner.runTests(Jtest.class);
    }

//    public static class Jtest {
//        @Test
//        public void printTest() {
//            Table a = new Table("A", new String[]{"name string", "age int"});
//            a.columns.get(0).values.add("Parsa");
//            a.columns.get(0).values.add("Bryce");
//            a.columns.get(1).values.add(20);
//            a.columns.get(1).values.add(21);
//            a.numberOfRows = 2;
//            String expected = "name string,age int\n" +
//                    "Parsa,20\n" +
//                    "Bryce,21";
//            String actual = a.print();
//            assertTrue("print test:", expected.equals(actual));
//        }
//
//        @Test
//        public void loadAndStoreTest() {
//            //TableManager tester = new TableManager();
//            //Table a = tester.load("t");
//            Database db = new Database();
//            String result = db.transact("load examples/records");
//            System.out.print(result);
//            //String expected = "ERROR: File not found exception";
//            //Table records = db.get("examples/teams");
//
//
//            String msg = db.transact("insert into examples/records values 'Golden Bears',2019,5,7,0");
//            System.out.println(msg);
//
//
//            //String[] arr = "'Golden Bears',2016,5,7,0".split(",");
//            //records.insertRow(arr);
//            Table a = new Table("examples/teams2", new String[]{"name string", "age int"});
//            a.columns.get(0).values.add("Parsa");
//            a.columns.get(0).values.add("Bryce");
//            a.columns.get(1).values.add(20);
//            a.columns.get(1).values.add(21);
//            a.numberOfRows = 2;
//
//            db.add("examples/teams2", a);
//
//            String storeRs = db.transact("store examples/teams");
//            //String storeRsoprint = records.print();
//            String test;
//
//            String result2 = db.transact("load examples/teams");
//            Table records2 = db.get("examples/teams");
////            System.out.println(records2.print());
//
//            //assertTrue("print test2:", expected.equals(result));
//        }
//
//        @Test
//        public void JoinTest(){
//            Table a = new Table("A", new String[]{"x int", "y int"});
//            Table b = new Table("B", new String[]{"x int", "z int"});
//            a.columns.get(0).values.add(2);
//            a.columns.get(0).values.add(8);
//            a.columns.get(0).values.add(13);
//
//            a.columns.get(1).values.add(5);
//            a.columns.get(1).values.add(3);
//            a.columns.get(1).values.add(7);
//
//            a.numberOfRows = 3;
//
//            b.columns.get(0).values.add(2);
//            b.columns.get(0).values.add(8);
//            b.columns.get(0).values.add(10);
//            b.columns.get(0).values.add(11);
//
//            b.columns.get(1).values.add(4);
//            b.columns.get(1).values.add(9);
//            b.columns.get(1).values.add(1);
//            b.columns.get(1).values.add(1);
//            b.numberOfRows = 4;
//
//            System.out.print(join(a, b).print()); //should have set consisting of rows 0 and 1 at the current point of completion.
//
//            System.out.println("T: " + '\n');
//            //second test
//
//            Table c = new Table("A", new String[]{"x int", "y int", "z int", "w int"});
//            Table d = new Table("B", new String[]{"w int", "b int", "z int"});
//            c.columns.get(0).values.add(1);
//            c.columns.get(0).values.add(7);
//            c.columns.get(0).values.add(1);
//
//            c.columns.get(1).values.add(7);
//            c.columns.get(1).values.add(7);
//            c.columns.get(1).values.add(9);
//
//            c.columns.get(2).values.add(2);
//            c.columns.get(2).values.add(4);
//            c.columns.get(2).values.add(9);
//
//            c.columns.get(3).values.add(10);
//            c.columns.get(3).values.add(1);
//            c.columns.get(3).values.add(1);
//
//            c.numberOfRows = 3;
//
//            d.columns.get(0).values.add(1);
//            d.columns.get(0).values.add(7);
//            d.columns.get(0).values.add(1);
//            d.columns.get(0).values.add(1);
//
//            d.columns.get(1).values.add(7);
//            d.columns.get(1).values.add(7);
//            d.columns.get(1).values.add(9);
//            d.columns.get(1).values.add(11);
//
//            d.columns.get(2).values.add(4);
//            d.columns.get(2).values.add(3);
//            d.columns.get(2).values.add(6);
//            d.columns.get(2).values.add(9);
//            d.numberOfRows = 4;
//
//            System.out.println(join(c, d).print());
//            System.out.println("T: " + '\n');
//            //*************************************
//
//            Table e = new Table("E", new String[]{"x int", "y int"});
//            Table f = new Table("F", new String[]{"x int", "z int"});
//            e.columns.get(0).values.add(1);
//            e.columns.get(0).values.add(2);
//            e.columns.get(0).values.add(1);
//
//            e.columns.get(1).values.add(4);
//            e.columns.get(1).values.add(5);
//            e.columns.get(1).values.add(6);
//
//            e.numberOfRows = 3;
//
//            f.columns.get(0).values.add(1);
//            f.columns.get(0).values.add(7);
//            f.columns.get(0).values.add(2);
//            f.columns.get(0).values.add(1);
//
//            f.columns.get(1).values.add(7);
//            f.columns.get(1).values.add(7);
//            f.columns.get(1).values.add(9);
//            f.columns.get(1).values.add(11);
//
//            f.numberOfRows = 4;
//            System.out.println("T2: " + '\n');
//
//            System.out.println(join(e, f).print());
//
//            System.out.println("E:" + e.print());
//            System.out.println("F:" + f.print());
//
//            Table g = new Table("G", new String[]{"x int", "y int", "z int"});
//            Table h = new Table("H", new String[]{"a int", "b int"});
//            g.columns.get(0).values.add(2);
//            g.columns.get(0).values.add(8);
//
//            g.columns.get(1).values.add(5);
//            g.columns.get(1).values.add(3);
//
//            g.columns.get(2).values.add(4);
//            g.columns.get(2).values.add(9);
//            g.numberOfRows = 2;
//
//            h.columns.get(0).values.add(7);
//            h.columns.get(0).values.add(2);
//
//            h.columns.get(1).values.add(0);
//            h.columns.get(1).values.add(8);
//
//            h.numberOfRows = 2;
//
//            Table i = new Table("G", new String[]{"x int", "y int", "z int"});
//            Table j = new Table("H", new String[]{"x int", "y int", "w int"});
//            i.columns.get(0).values.add(1);
//            i.columns.get(0).values.add(1);
//
//            i.columns.get(1).values.add(2);
//            i.columns.get(1).values.add(3);
//
//            i.columns.get(2).values.add(1);
//            i.columns.get(2).values.add(2);
//
//            i.numberOfRows = 2;
//
//            j.columns.get(0).values.add(1);
//            j.columns.get(0).values.add(1);
//
//            j.columns.get(1).values.add(2);
//            j.columns.get(1).values.add(3);
//
//            j.columns.get(2).values.add(3);
//            j.columns.get(2).values.add(4);
//
//            j.numberOfRows = 2;
//
//            System.out.println(join(i, j).print());
//
//        }
//
//
//        public static void main(String[] args) {
//            jh61b.junit.TestRunner.runTests(Jtest.class);
//        }
//    }

    public Column getCol(int i) {
        if (i > columns.size() || i < 0) {
            return null;
        }
        return this.columns.get(i);
    }

    /** Returns the join of all the Tables */
//    public static Table join(Table t1, Table t2){
//        Table result;
//        Stream<Column> mutualColStream = t1.columns.stream().filter(c -> t2.col_indices.containsKey(c.name()));
//        Object[] a = mutualColStream.toArray();
//
//
////        long mutualCols = mutualColStream.count();
////        if (mutualCols > 0) {
////
////        } else {
////
////        }
//
//    }

    public Table selectExpr(String exprs) throws QueryException{
        // TODO: 3/4/2017 check to see if the column names exist
        if (exprs.equals("*")) return this;
        Table result = new Table(this.name(), null);
        String[] colExprs = exprs.split("\\s*,\\s*");
        for (String ce : colExprs) {
            String[] as = ce.split("\\s+as\\s+");
            if (as.length == 1 && col_indices.get(ce) != null) {
                result.addColumn(columns.get(col_indices.get(ce)));
            } else {
                Pattern operator = Pattern.compile("(\\w+)\\s*([+\\-*/])\\s*(\\w+)");
                Matcher m = operator.matcher(as[0]);
                if (as.length == 2 && m.matches()){
                    StringJoiner temp = new StringJoiner(" ");
                    for (int i = 1; i <= 3; i++) temp.add(m.group(i));
                    result.addColumn(columnOperation(temp.toString().split(" "), as[1]));
                } else if (as.length == 2) {
                    result.addColumn(columnOperation(new String[]{as[0]}, as[1]));
                } else {
                    throw new QueryException(String.format("ERROR: invalid expression: %s\n", ce));
                }
            }
        }
        result.numberOfRows = result.columns.get(0).size();
        result.setCtorCols();
        return result;
    }

    private void addColumn(Column other){
        Column newColumn = new Column(other.name(),other.type());
        newColumn.values = new ArrayList(other.values());
        this.columns.add(newColumn);
        this.col_indices.put(newColumn.name(), this.columns.size() - 1);
        this.numberOfRows = newColumn.values.size();
    }

    private Column columnOperation(String[] operation, String name) throws QueryException{
        if (operation.length == 3 && col_indices.get(operation[0]) != null){
            Column column1 = getCol(col_indices.get(operation[0])), column2;
            if (column1.type().equalsIgnoreCase("string")){
                if (operation[2].startsWith("'") && operation[1].equals("+")){
                    Column newColumn = new Column(name, "string");
                    for (Object x : column1.values) {
                        if (x.equals(NOVALUE)) {
                            x = "";
                        } else {
                            x = ((String)x).substring(0,((String) x).length()- 1);
                        }
                        newColumn.values.add((String)x + operation[1].substring(1));
                    }
                    return newColumn;
                } else if (col_indices.get(operation[2]) != null && (column2 = getCol(col_indices.get(operation[2]))).type().equalsIgnoreCase("string") && operation[1].equals("+")) {
                    Column newColumn = new Column(name, "string");
                    for (int i = 0; i < numberOfRows; i++) {
                        String first = (String)column1.getItem(i);
                        String second = (String)column2.getItem(i);
                        String result;
                        if (first.equals(NOVALUE) && second.equals(NOVALUE)){
                            result = NOVALUE;
                        } else if (first.equals(NOVALUE)) {
                            result = second;
                        } else if (second.equals(NOVALUE)){
                            result = first;
                        } else {
                            result = first.substring(0,first.length() - 1) + second.substring(1);
                        }
                        newColumn.values.add(result);
                    }
                    return newColumn;
                }
            } else if (col_indices.get(operation[2]) != null) {
                column2 = getCol(col_indices.get(operation[2]));
                if (column2.type().equalsIgnoreCase("string")) throw new QueryException("ERROR: adding string to number\n");
                Column newColumn = new Column(name, (column1.type().equalsIgnoreCase("float") || column2.type().equalsIgnoreCase("float")) ? "float" : "int");
                for (int i = 0; i < numberOfRows; i++) {
                    newColumn.values.add(apply((column1).getItem(i), column1.type(), (column2).getItem(i), column2.type(), operation[1]));
                }
                return newColumn;
            } else if (col_indices.get(operation[2]) == null && !operation[2].startsWith("'")){
                Column newColumn;
                try {
                    Integer val = Integer.valueOf(operation[2]);
                    newColumn = new Column(name, column1.type().equalsIgnoreCase("int") ? "int" : "float");
                    for (int i = 0; i < numberOfRows; i++) {
                            newColumn.values.add(apply((column1).getItem(i), column1.type(), val, "float", operation[1]));
                    }
                } catch (NumberFormatException e) {
                    try {
                        Float val = Float.valueOf(operation[2]);
                        newColumn = new Column(name, "float");
                        for (int i = 0; i < numberOfRows; i++) {
                            newColumn.values.add(apply((column1).getItem(i), column1.type(), val, "float", operation[1]));
                        }
                    } catch(NumberFormatException n) {
                        throw new QueryException(String.format("ERROR: unauthorized symbol: %s\n", operation[2]));
                    }
                }
            }
        } else if (col_indices.get(operation[0]) != null) {
            return getCol(col_indices.get(operation[0]));
        }
        throw new QueryException("ERROR: bad select expression\n");
    }

    private static Object apply(Object first, String fType, Object second, String sType, String operator) throws QueryException {
        try {
        if (first.equals(NOVALUE) && second.equals(NOVALUE)) return NOVALUE;
        else if (first.equals(NOVALUE)) first = (fType.equalsIgnoreCase("int") ? 0 : (float)0.0);
        else if (second.equals(NOVALUE)) second = (sType.equalsIgnoreCase("int") ? 0 : (float)0.0);
        if (first.equals(NAN) || second.equals(NAN)) return NAN;
        if (fType.equalsIgnoreCase("int") && sType.equalsIgnoreCase("float")) {
            return apply((Integer)first, (Float)second, operator);
        } else if (fType.equalsIgnoreCase("float") && sType.equalsIgnoreCase("int")) {
            return apply((Float)first, (Integer)second, operator);
        } else if (fType.equalsIgnoreCase("int") && sType.equalsIgnoreCase("int")) {
            return apply((Integer)first, (Integer)second, operator);
        } else if (fType.equalsIgnoreCase("float") && sType.equalsIgnoreCase("float")) {
            return apply((Float) first, (Float) second, operator);
        }
        }catch(ArithmeticException e) {
            throw new QueryException("ERROR: Number Overflow");
        }
        throw new QueryException("ERROR: bad types \n");
    }

    private static Object apply(Float a, Float b, String operator) throws QueryException{
        Object result;
        switch (operator) {
            case "+": result = (a + b);
                break;
            case "*": result = (a * b);
                break;
            case "-": result = (a - b);
                break;
            case "/": result = (Float.isInfinite(a / b) || Float.isNaN(a / b) ? NAN : a / b);
                break;
            default: throw new QueryException(String.format("ERROR: invalid operator %s\n", operator));
        }
        return result;
    }
    private static Object apply(Integer a, Integer b, String operator) throws QueryException{
        Object result = apply((float)(a/1.0),(float)(b/1.0),operator);
        if(result.equals(NAN)){
            return result;
        }
        return Math.toIntExact(Math.round(Math.floor((Float)result)));
    }
    private static Object apply(Integer a, Float b, String operator) throws QueryException{
        return apply((float)(a/1.0),b,operator);
    }
    private static Object apply(Float a, Integer b, String operator) throws QueryException{
        return apply(a,(float)(b/1.0),operator);
    }


    public Table selectRows(String operand1, String comparator, String operand2) throws  QueryException{
        Table result = new Table(this.name(), this.ctorCols);
        if (this.col_indices.containsKey(operand1)) {
            Column column1 = this.columns.get(this.col_indices.get(operand1));
            if (this.col_indices.containsKey(operand2)) {
                Column column2 = this.columns.get(this.col_indices.get(operand2));
                for (int i = 0; i < this.numberOfRows; i++) {
                    if (comparison(column1.getItem(i), column1.type(), column2.getItem(i), column2.type(), comparator)) {
                        result.insertRow(this.dataStringRow(i).split("\\s*,\\s*"));
                    }
                }
                return  result;
            } else {
                try {
                    String type;
                    if (operand2.startsWith("'") && operand2.endsWith("'")) {
                        type = "string";
                    } else if (Float.valueOf(operand2) != null){
                        operand2 = String.valueOf(Float.valueOf(operand2));
                        type = "float";
                    } else {
                        throw new NumberFormatException();
                    }
                    for (int i = 0; i < this.numberOfRows; i++) {
                        if (comparison(column1.getItem(i), column1.type(), operand2, type, comparator)) {
                            result.insertRow(this.dataStringRow(i).split("\\s*,\\s*"));
                        }
                    }
                    return result;
                } catch (NumberFormatException r){}
                throw new QueryException(String.format("ERROR: invalid operand: in condition: %s not found\n", operand2, operand1 + comparator + operand2));
            }
        } else {
            throw new QueryException(String.format("ERROR: column name: %s in condition: %s not found\n", operand1, operand1 + comparator + operand2));
        }
    }

    private static boolean comparison(Object first, String fType, Object second, String sType, String comparator) throws QueryException{
        if (fType.equalsIgnoreCase("string") && sType.equalsIgnoreCase("string")){
            return comparison((String)first,(String)second,comparator);
        } else if (fType.equalsIgnoreCase("int") && sType.equalsIgnoreCase("int")){
            if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
                return false;
            } else if (first.equals(NAN) && ! second.equals(NAN)) {
                second = (float)0.0;
                first = (float)1.0;
            } else if (second.equals(NAN) && ! first.equals(NAN)){
                first = (float)0.0;
                second = (float)1.0;
            } else if (second.equals(NAN) && first.equals(NAN)) {
                return comparison((String)first,(String)second,comparator);
            }
            return comparison((float)((Integer)first / 1.0),(float)((Integer)second/ 1.0),comparator);
        } else if (fType.equalsIgnoreCase("float") && sType.equalsIgnoreCase("float")){
            if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
                return false;
            } else if (first.equals(NAN) && ! second.equals(NAN)) {
                second = (float)0.0;
                first = (float)1.0;
            } else if (second.equals(NAN) && ! first.equals(NAN)){
                first = (float)0.0;
                second = (float)1.0;
            } else if (second.equals(NAN) && first.equals(NAN)) {
                return comparison((String)first,(String)second,comparator);
            }
            return comparison((Float)first,(Float)second,comparator);
        } else if (fType.equalsIgnoreCase("float") && sType.equalsIgnoreCase("int")){
            if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
                return false;
            } else if (first.equals(NAN) && ! second.equals(NAN)) {
                second = (float)0.0;
                first = (float)1.0;
            } else if (second.equals(NAN) && ! first.equals(NAN)){
                first = (float)0.0;
                second = (float)1.0;
            } else if (second.equals(NAN) && first.equals(NAN)) {
                return comparison((String)first,(String)second,comparator);
            }
            return comparison((Float)first,(float)((Integer)second/ 1.0),comparator);
        } else if (fType.equalsIgnoreCase("int") && sType.equalsIgnoreCase("float")){
            if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
                return false;
            } else if (first.equals(NAN) && ! second.equals(NAN)) {
                second = (float)0.0;
                first = (float)1.0;
            } else if (second.equals(NAN) && ! first.equals(NAN)){
                first = (float)0.0;
                second = (float)1.0;
            } else if (second.equals(NAN) && first.equals(NAN)) {
                return comparison((String)first,(String)second,comparator);
            } else if (second.toString().startsWith("'") && second.toString().endsWith("'")) {
                return false;
                //return comparison(()first,(String)second,comparator);
            }
            return comparison((float)((Integer)first / 1.0),(Float)second,comparator);
        }
        throw new QueryException(String.format("ERROR: incompatible types in select condition: %s\n", first.toString() + ", " + second.toString()));
    }

    private static boolean comparison(String first, String second, String comparator) throws QueryException {
        if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
            return false;
        } else if (first.equals(NAN) && ! second.equals(NAN)) {
            second = "";
        } else if (second.equals(NAN) && ! first.equals(NAN)){
            first = "";
        }
        switch (comparator) {
            case "==": return first.compareTo(second) == 0;
            case "!=": return first.compareTo(second) != 0;
            case "<": return first.compareTo(second) < 0;
            case ">": return first.compareTo(second) > 0;
            case "<=": return first.compareTo(second) < 0 || first.compareTo(second) == 0;
            case ">=": return first.compareTo(second) > 0 ||  first.compareTo(second) == 0;
            default: throw new QueryException(String.format("ERROR: invalid condition: %s\n", comparator));
        }
    }
    private static boolean comparison(Float first, Float second, String comparator) throws QueryException {

        switch (comparator) {
            case "==": return first.compareTo(second) == 0;
            case "!=": return first.compareTo(second) != 0;
            case "<": return first.compareTo(second) < 0;
            case ">": return first.compareTo(second) > 0;
            case "<=": return first.compareTo(second) < 0 || first.compareTo(second) == 0;
            case ">=": return first.compareTo(second) > 0 ||  first.compareTo(second) == 0;
            default: throw new QueryException(String.format("ERROR: invalid condition: %s\n", comparator));
        }
    }

//    private static boolean comparison(Float first, Float second, String comparator) throws QueryException {
//        if (first.equals(NOVALUE) || second.equals(NOVALUE)) {
//            return false;
//        } else if (first.equals(NAN) && ! second.equals(NAN)) {
//            second = 0.0;
//        } else if (second.equals(NAN) && ! first.equals(NAN)){
//            first = 0.0;
//        }
//        switch (comparator) {
//            case "==": return first.compareTo(second) == 0;
//            case "!=": return first.compareTo(second) != 0;
//            case "<": return first.compareTo(second) < 0;
//            case ">": return first.compareTo(second) > 0;
//            case "<=": return first.compareTo(second) < 0 || first.compareTo(second) == 0;
//            case ">=": return first.compareTo(second) > 0 ||  first.compareTo(second) == 0;
//            default: throw new QueryException(String.format("ERROR: invalid condition: %s\n", comparator));
//        }
//    }

    public static String[] joinedColNames(Table t1, Table t2){
        ArrayList<String> t1Names = t1.columnNameAndTypeList(); //set of the keys in this table
        ArrayList<String> t2Names = t2.columnNameAndTypeList();
        ArrayList<String> t3Names = new ArrayList<>(t1Names);

        t3Names.retainAll(t2Names);

        t1Names.removeAll(t3Names);
        t2Names.removeAll(t3Names);

        t3Names.addAll(t1Names);
        t3Names.addAll(t2Names);

        //String[] JoinedNames = (String[]) t3Names.toArray();

        String[] JoinedNames = Arrays.copyOf(t3Names.toArray(), t3Names.toArray().length, String[].class);

        return JoinedNames;
    }

    public void RowInsert(Table t1, Table t2, int row1, int row2){
        ArrayList<String> t1ColNames = t1.columnNameList();
        ArrayList<String> t2ColNames = t2.columnNameList();

        t2ColNames.removeAll(t1ColNames);

        for(int i = 0; i < t1ColNames.size(); i++){
            int index1 = this.IndexFinder((String) t1ColNames.get(i));
            Column hold = this.getCol(index1);

            int t1_index1 = t1.IndexFinder((String) t1ColNames.get(i));
            Object item = t1.getCol(t1_index1).getItem(row1);

            hold.insertItem(item);

            this.ColumnSet(hold);
        }

        for(int x = 0; x < t2ColNames.size(); x++){
            int index2 = this.IndexFinder((String) t2ColNames.get(x));
            Column hold = this.getCol(index2);

            int t2_index2 = t2.IndexFinder((String) t2ColNames.get(x));
            Object item = t2.getCol(t2_index2).getItem(row2);

            hold.insertItem(item);

            this.ColumnSet(hold);
        }

        this.numberOfRows+=1;

    }

    /**
     * Returns the join of all the Tables
     */
    public static Table join(Table t1, Table t2) {
        ArrayList<String> t1Set = t1.columnNameList(); //set of the keys in this table ONLY NAMES
        ArrayList<String> t2Set = t2.columnNameList(); //

        Set t1RowSet = new LinkedHashSet();
        //will be used to store the indices of the rows that we need with this table;
        Set t2RowSet = new LinkedHashSet();


        t1Set.retainAll(t2Set); //now t1_set will have the intersection of the column name sets in ORDER based on t1's ordering

        if (t1Set.size() == 0) {
            //This means that there are no columns in either table that intersect by name
            //we need a full cartesian product
            return FullCartesian(t1, t2);
        }

        Object[] colNames = t1Set.toArray();

        for (int i = 0; i < colNames.length; i++) {
            String name = colNames[i].toString();
            int t1Pos = t1.IndexFinder(name);
            int t2Pos = t2.IndexFinder(name);

            Set t1LocalRowSet = new LinkedHashSet();
            Set t2LocalRowSet = new LinkedHashSet();

            //might make more sense to RetainAll on the column lists, then look for the elements in each column. Then populate eah table's match set.
            //the above comment is in comparison to my notes and comparing each element to al the others etc.

            LinkedHashSet<Object> c3 = new LinkedHashSet<>(t1.getCol(t1Pos).values());

            c3.retainAll(t2.getCol(t2Pos).values()); //should hopefully return the intersection of the values in these matching columns

            if (c3.size() == 0) {
                //This means that a matched column has no matching elements with its twin in
                return new Table("name", joinedColNames(t1, t2));
            }

            //now go through this intersection and populate each table's set of indices.

            for (int x = 0; x < c3.size(); x++) {

                for (int u = 0; u < t1.getCol(t1Pos).size(); u++) {
                    ArrayList<Object> temp1 = t1.getCol(t1Pos).values();

                    if (c3.contains(temp1.get(u))) {
                        //then we add this column of t1 for the set in the table
                        t1LocalRowSet.add(u); //u is the index of c3.get(x) in the first table's matched column
                    }
                }

                for (int w = 0; w < t2.getCol(t2Pos).size(); w++) {
                    ArrayList<Object> temp2 = t2.getCol(t2Pos).values();

                    if (c3.contains(temp2.get(w))) {
                        //then we add this column of t1 for the set in the table
                        t2LocalRowSet.add(w); //u is the index of c3.get(x) in the first table's matched column
                    }
                }
            }

            //now after we've gone through this first column, we need to initialize the table's row set to be the set of the first column.

            if (i == 0) {
                //then in this case we want to set the table's set to initially be the first column's set
                t1RowSet = t1LocalRowSet;
                t2RowSet = t2LocalRowSet;
            } else {
                //otherwise, we want to intersect the local rowset and table row set
                t1RowSet.retainAll(t1LocalRowSet);
                t2RowSet.retainAll(t2LocalRowSet);
            }

        }//end of loop through columns


        if(t1RowSet.size() == 0 || t2RowSet.size() == 0){
            return new Table("name", joinedColNames(t1, t2));
        }

        //now at this point, we should have the table sets that contain the rows for each columns that we need.
        //if either of the sets' size are equal to zero, then the join will just make empty columns.

        Table joinedTable = new Table("name", joinedColNames(t1, t2));
        //NOW we have constructed the joined table but data entry is required at this point.
        Object[] t1Rows = t1RowSet.toArray();
        String match_col = (String) colNames[0];
        int index1 = t1.col_indices.get(match_col);
        Column hold1 = t1.getCol(index1); //copy of first matching column from t1

        int index2 = t2.col_indices.get(match_col);
        Column hold2 = t2.getCol(index2);

        for(int i = 0; i < t1Rows.length; i++){
            for(int x = 0; x < hold2.size(); x++){
                if(rowMatch(t1, t2, (Integer) t1Rows[i], x, colNames)){
                    //when this is true, add row to the table from both of these rows.
                    joinedTable.RowInsert(t1, t2, (Integer) t1Rows[i], x);
                }
            }
        }

        return joinedTable;
    }//end of func

    public static boolean rowMatch(Table t1, Table t2, int r1, int r2, Object[] colNames){
        boolean rowTruth = true;
        for(int i = 0; i < colNames.length; i++){
            int t1_ind = t1.IndexFinder(colNames[i].toString());
            Column t1_col = t1.getCol(t1_ind);

            int t2_ind = t2.IndexFinder(colNames[i].toString());
            Column t2_col = t2.getCol(t2_ind);

            if(!t1_col.getItem(r1).equals(t2_col.getItem(r2))){
                rowTruth = false;
            }
        }
        return rowTruth;
    }


    public static Table FullCartesian(Table t1, Table t2){
        ArrayList<String> t1Names = t1.columnNameAndTypeList(); //set of the keys in this table
        ArrayList<String> t2Names = t2.columnNameAndTypeList();
        ArrayList<String> t3Names = new ArrayList<>(t1Names);

        t3Names.addAll(t2Names);

        String[] JoinedNames = Arrays.copyOf(t3Names.toArray(), t3Names.toArray().length, String[].class);

        Table joinedTable = new Table("name", JoinedNames);
        //now we have the joined table that should be just columns of t1 + t2

        ArrayList<String> t1NameSet = t1.columnNameList(); //set of the keys in this table ONLY NAMES
        ArrayList<String> t2NameSet = t2.columnNameList();

        Object[] t1NameSetArr = t1NameSet.toArray(); //should contain unique columns in t1
        Object[] t2NameSetArr = t2NameSet.toArray(); //should contain unique columns in t2

        int t1_num_of_rows = t1.num_rows();
        int t2_num_of_rows = t2.num_rows();

        for(int i = 0; i < t1NameSetArr.length; i++){
            String name = (String) t1NameSetArr[i];
            int index = t1.col_indices.get(name);
            Column hold = t1.getCol(index);
            //make a function in column that returns the same column but with each row copied a certain # of times
            Column newCol = hold.rowMult(t2_num_of_rows);
            joinedTable.ColumnSet(newCol);
        }

        for(int i = 0; i < t2NameSetArr.length; i++){
            String name = (String) t2NameSetArr[i];
            int index = t2.col_indices.get(name);
            Column hold = t2.getCol(index);
            Column newCol = hold.valuesMult(t1_num_of_rows);
            joinedTable.ColumnSet(newCol);
        }

        joinedTable.numRowsSetter(t1_num_of_rows * t2_num_of_rows);

        return joinedTable;

    }

    public static Table PartialCartesian(Table t1, Table t2, Set t1RowSet, Set t2RowSet){
        int t1_num_of_rows = t1.num_rows();
        int t2_num_of_rows = t2.num_rows();

        int t1_match_rows = t1RowSet.size();
        int t2_match_rows = t2RowSet.size();

        //create a table helper function that returns a cpoy of the table with
        //all of the rows not in the rows set removed OR RATHER, keep only the rows in the row set.
        ArrayList<String> t1Names = t1.columnNameAndTypeList(); //set of the keys in this table
        ArrayList<String> t2Names = t2.columnNameAndTypeList();
        ArrayList<String> t3Names = new ArrayList<>(t1Names);

        t3Names.retainAll(t2Names);

        t1Names.removeAll(t3Names);
        t2Names.removeAll(t3Names);

        t3Names.addAll(t1Names);
        t3Names.addAll(t2Names);

        //String[] JoinedNames = (String[]) t3Names.toArray();

        String[] JoinedNames = Arrays.copyOf(t3Names.toArray(), t3Names.toArray().length, String[].class);

        Table joinedTable = new Table("name", JoinedNames);

        return joinedTable;


    }
}

