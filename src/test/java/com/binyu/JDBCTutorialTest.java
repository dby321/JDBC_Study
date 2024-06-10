package com.binyu;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import java.util.stream.IntStream;

/**
 * JDBC Tutorial
 * https://www.baeldung.com/java-jdbc
 *
 * <dependency>
 * <groupId>mysql</groupId>
 * <artifactId>mysql-connector-java</artifactId>
 * <version>8.0.32</version>
 * </dependency>
 */
@Slf4j
public class JDBCTutorialTest {
   
    /**
     * 测试数据库连接。
     * <p>
     * 本方法旨在验证是否能够成功建立与MySQL数据库的连接。通过在代码中指定数据库URL、用户名和密码，
     * 使用DriverManager获取数据库连接。如果连接失败，将抛出RuntimeException。
     *
     * @throws ClassNotFoundException 如果MySQL JDBC驱动程序类未找到，将抛出此异常。
     * @throws SQLException           如果建立数据库连接失败，将抛出此异常。
     */
    @Test
    public void testStatement() throws ClassNotFoundException {
        // 加载MySQL JDBC驱动程序类
        Class.forName("com.mysql.cj.jdbc.Driver");
        String selectSql = "SELECT * FROM employees";
        // 尝试建立数据库连接，并在try-with-resources语句中自动管理连接的关闭
        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");
             Statement stmt = con.createStatement();
             ResultSet resultSet = stmt.executeQuery(selectSql);) {
            // 连接成功后，可以在这里执行进一步的数据库操作
//            String tableSql = "CREATE TABLE IF NOT EXISTS employees"
//                    + "(emp_id int PRIMARY KEY AUTO_INCREMENT, name varchar(30),"
//                    + "position varchar(30), salary double)";
//            stmt.execute(tableSql);
            String insertSql = "INSERT INTO employees(name, position, salary)"
                    + " VALUES('john', 'developer', 2000)";
            stmt.executeUpdate(insertSql);
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                Employee emp = new Employee();
                emp.setId(resultSet.getInt("emp_id"));
                emp.setName(resultSet.getString("name"));
                emp.setPosition(resultSet.getString("position"));
                emp.setSalary(resultSet.getDouble("salary"));
                employees.add(emp);
            }
        } catch (SQLException e) {
            // 如果在获取或使用连接时发生错误，将其包装并抛出RuntimeException
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPreparedStatement() throws ClassNotFoundException {
        // 加载MySQL JDBC驱动程序类
        Class.forName("com.mysql.cj.jdbc.Driver");
        String updatePositionSql = "UPDATE employees SET position=? WHERE emp_id=?";
        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");
             PreparedStatement pstmt = con.prepareStatement(updatePositionSql);
        ) {
            pstmt.setString(1, "developer");
            pstmt.setInt(2, 1);
            int rowAffected = pstmt.executeUpdate();
        } catch (SQLException e) {
            // 如果在获取或使用连接时发生错误，将其包装并抛出RuntimeException
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试CallableStatement的使用。
     * 该方法旨在通过模拟调用数据库存储过程或函数，验证CallableStatement的功能和正确性。
     * TODO 省略，不用存储过程
     *
     * @throws ClassNotFoundException 如果类没有找到，这个异常会被抛出。
     *                                这里模拟了可能在实际数据库连接或准备语句过程中遇到的类加载问题。
     */
    @Test
    public void testCallableStatement() throws ClassNotFoundException {

    }

    /**
     * 测试可更新结果集的功能。
     * 本测试方法演示了如何使用JDBC连接到MySQL数据库，并通过可更新结果集插入新行。
     *
     * @throws ClassNotFoundException 如果找不到MySQL JDBC驱动程序类。
     */
    @Test
    public void testUpdatableResultSet() throws ClassNotFoundException {
        // 加载MySQL JDBC驱动程序类，以便与MySQL数据库建立连接。
        // 加载MySQL JDBC驱动程序类
        Class.forName("com.mysql.cj.jdbc.Driver");

        // 定义SQL查询语句，用于从employees表中选择所有行。
        String selectSql = "select * from employees";

        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");
             // 创建一个可更新的声明，允许对结果集进行读写操作。
             Statement updatableStmt = con.createStatement(
                     ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet updatableResultSet = updatableStmt.executeQuery(selectSql)
        ) {
            // 移动到插入行，准备插入新数据。
            updatableResultSet.moveToInsertRow();

            // 更新插入行的各列值，这里以name、position和salary为例。
            updatableResultSet.updateString("name", "mark");
            updatableResultSet.updateString("position", "analyst");
            updatableResultSet.updateDouble("salary", 2000);

            // 将插入行插入到结果集中。
            updatableResultSet.insertRow();
        } catch (SQLException e) {
            // 如果在处理数据库连接或操作时发生SQL异常，将其转换为运行时异常并抛出。
            // 如果在获取或使用连接时发生错误，将其包装并抛出RuntimeException
            throw new RuntimeException(e);
        }
    }

    /**
     * The JDBC API allows looking up information about the database, called metadata.
     * JDBC API允许查找有关数据库的信息，称为元数据。
     * <p>
     * The DatabaseMetadata interface can be used to obtain general information about the database such as the tables, stored procedures, or SQL dialect.
     * DatabaseMetadata接口可用于获取有关数据库的常规信息，如表、存储过程或SQL方言。
     * <p>
     * Let’s have a quick look at how we can retrieve information on the database tables:
     * 让我们快速了解一下如何检索数据库表上的信息：
     *
     * @throws ClassNotFoundException
     */
    @Test
    public void testDatabaseMetaData() throws ClassNotFoundException {
        // 加载MySQL JDBC驱动程序类，以便与MySQL数据库建立连接。
        // 加载MySQL JDBC驱动程序类
        Class.forName("com.mysql.cj.jdbc.Driver");

        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");
             // 创建一个可更新的声明，允许对结果集进行读写操作。

        ) {
            DatabaseMetaData dbmd = con.getMetaData();
            ResultSet tablesResultSet = dbmd.getTables(null, null, "%", null);
            while (tablesResultSet.next()) {
                log.info(tablesResultSet.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            // 如果在处理数据库连接或操作时发生SQL异常，将其转换为运行时异常并抛出。
            // 如果在获取或使用连接时发生错误，将其包装并抛出RuntimeException
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试ResultSetMetaData类的使用。
     * ResultSetMetaData用于获取结果集中的元数据信息，例如列数、列名等。
     * 本测试方法演示了如何使用ResultSetMetaData来获取并打印结果集的列名。
     *
     * @throws ClassNotFoundException 如果驱动程序类未找到，则抛出此异常。
     */
    @Test
    public void testResultSetMetaData() throws ClassNotFoundException {
        // 加载MySQL JDBC驱动程序类
        Class.forName("com.mysql.cj.jdbc.Driver");

        String selectSql = "SELECT * FROM employees";
        // 尝试建立数据库连接，并在try-with-resources语句中自动管理连接的关闭
        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");
             Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql);) {
            // 获取结果集的元数据
            ResultSetMetaData rsmd = rs.getMetaData();
            // 获取结果集的列数
            int nrColumns = rsmd.getColumnCount();

            // 遍历所有列，打印列名
            IntStream.range(1, nrColumns).forEach(i -> {
                try {
                    // 使用列索引获取列名，并通过日志记录下来
                    log.info(rsmd.getColumnName(i));
                } catch (SQLException e) {
                    // 处理SQL异常
                    e.printStackTrace();
                }
            });

        } catch (SQLException e) {
            // 如果在处理数据库连接或操作时发生SQL异常，将其转换为运行时异常并抛出。
            // 如果在获取或使用连接时发生错误，将其包装并抛出RuntimeException
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTransaction() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/jdbc?serverTimezone=UTC", "root", "root");

        ) {
            String updatePositionSql = "UPDATE employees SET position=? WHERE emp_id=?";
            PreparedStatement pstmt = con.prepareStatement(updatePositionSql);
            pstmt.setString(1, "lead developer");
            pstmt.setInt(2, 1);

            String updateSalarySql = "UPDATE employees SET salary=? WHERE emp_id=?";
            PreparedStatement pstmt2 = con.prepareStatement(updateSalarySql);
            pstmt2.setDouble(1, 3000);
            pstmt2.setInt(2, 1);

            boolean autoCommit = con.getAutoCommit();
            try {
                con.setAutoCommit(false);
                int i = pstmt.executeUpdate();
                int i1 = pstmt2.executeUpdate();

                log.info("Rows updated: {}", i);
                log.info("Rows updated: {}", i1);
                con.commit();

            } catch (SQLException exc) {
                log.error("Transaction failed");
                con.rollback();

            } finally {
                con.setAutoCommit(autoCommit);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
