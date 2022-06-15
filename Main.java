package com.company;
import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class Main {


    public static void main(String[] args) throws SQLException, IOException {

        //TODO сделать чтобы не было ошибок с log4j
        InputStream input = new FileInputStream(".\\config.properties");

        Properties properties = new Properties();
        properties.load(input);

        String login = properties.getProperty("login");
        String password = properties.getProperty("password");
        String DB_URL = properties.getProperty("DB_URL");

        String aggr = "SELECT  count(*) as COUNT, 'param1' as Datee from pages where app_name = 'UFR'\n" +
                "and module = 'ufr-coa' " +
                "and tier = 'AppServer' " +
                " and page_name not like '/ufr-coa-ui%' " +
                "and date_time > toDateTime('param1') and date_time < toDateTime('param2')";

        String pages = "SELECT  count(*) as COUNT, 'param1' as Datee, page_name as PAGENAME from pages where app_name = 'UFR'\n" +
                "and module = 'ufr-coa' " +
                "and tier = 'AppServer' " +
                " and page_name not like '/ufr-coa-ui%' " +
                "and date_time > toDateTime('param1') and date_time < toDateTime('param2')"+
                "group by page_name order by COUNT desc";

        String hours[] = new String[]
                {
                        "08:00:00",
                        "09:00:00",
                        "10:00:00",
                        "11:00:00",
                        "12:00:00",
                        "13:00:00",
                        "14:00:00",
                        "15:00:00",
                        "16:00:00",
                        "17:00:00",
                        "18:00:00",
                        "19:00:00",
                        "20:00:00",
                        "21:00:00",
                };


        int k = 10;
        int max = -1;
        String maxtimeStart = "";
        String maxtimeEnd = "";

        System.out.println("Соединение с кликом");
        Connection conn = DriverManager.getConnection(DB_URL, login, password);
        System.out.println("С кликом соединились, формируем и кидаем запросы");
        System.out.println("Количество отправленных запросов");
        while (k > 0) {

            for (int i = 0; i < hours.length - 1; i++) {
                String query = aggr
                        .replace("param1", LocalDate.now().minusDays(k) + " " + hours[i])
                        .replace("param2", LocalDate.now().minusDays(k) + " " + hours[i + 1]);


                try (PreparedStatement statement = conn.prepareStatement(query)) {
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            if (rs.getInt("COUNT") > max) {
                                max = rs.getInt("COUNT");
                                maxtimeStart = LocalDate.now().minusDays(k) + " " + hours[i];
                                maxtimeEnd = LocalDate.now().minusDays(k) + " " + hours[i + 1];
                            }
                        }
                    }
                }

            }
            k--;
            System.out.print("*");
        }


        conn.close();

        System.out.println();
        System.out.println("результат аггргирующего запроса " + max + " " + maxtimeStart + " " + maxtimeEnd);

        LinkedList<String> list = new LinkedList<String>();

        String query = pages
                .replace("param1", maxtimeStart)
                .replace("param2", maxtimeEnd);

        System.out.println( "НАчинаем кидать результирующий запрос " + query);

        conn = DriverManager.getConnection(DB_URL, login, password);

        int summ =0;
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    summ = summ +Integer.parseInt(String.valueOf(rs.getInt("COUNT")));
                    list.add(rs.getString("Datee")+";"+rs.getInt("COUNT")+";"+rs.getString("PAGENAME"));
                }
            }
        }
        conn.close();

        LinkedList<String> list1 = new LinkedList<String>();
        double accum = 0;
        for(String a : list){
            accum = accum + (Double.parseDouble(a.split(";")[1]) + 0.0) / summ * 100;
            list1.add(a + ";" + accum);
        }

        System.out.println( "Записываем в коллекцию");
        File f = new File("\\\\up\\Projects\\SKP\\COA\\ProfileCOA" +LocalDate.now().toString() +".txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));

        //bw.write("Date" + ";" +"COUNT"+";" + "URL" +";accum");
        System.out.println( "Записываем в файл");
        for(String ss : list){
            //System.out.println(ss);
            bw.write(ss +"\n");
        }

        bw.flush();
        bw.close();



        System.out.println( "Записываем в коллекцию новую");
        File f1 = new File("\\\\up\\Projects\\SKP\\COA\\ProfileCOAModify" +LocalDate.now().toString() +".txt");
        BufferedWriter bw1 = new BufferedWriter(new FileWriter(f1));

        bw1.write("Date" + ";" +"COUNT"+";" + "URL" +";accum\n");
        System.out.println( "Записываем в файл");
        for(String ss : list1){
            //System.out.println(ss);
            bw1.write(ss +"\n");
        }

        bw1.flush();
        bw1.close();
    }
}
