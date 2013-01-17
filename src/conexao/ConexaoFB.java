/*
 * Created on 09/08/2004
 */

package conexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author BBAmbrozio
 *
 * Classe respons�vel pela conex�o com o banco de dados Firebird 1.5
 *
 * Database: horas
 * User: sysdba
 * Password: masterkey
 * 
 */

public class ConexaoFB {

   public Connection con = null;
   public Statement stm = null;

   public ConexaoFB() {

      try {

         Class.forName("org.firebirdsql.jdbc.FBDriver");
         con =
            DriverManager.getConnection(
               "jdbc:firebirdsql:localhost/3050:C:\\Tmp\\DB.gdb",
               "sysdba",
               "masterkey");
         stm = con.createStatement();

      } catch (Exception e) {
         System.out.println("N�o foi poss�vel conectar ao banco: " + e.getMessage());
      }

   }
      
}