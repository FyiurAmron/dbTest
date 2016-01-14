package vax.db;

import java.io.*;
import java.sql.*;
import java.util.Random;

public class Main {
    /**
     @param args the command line arguments
     @throws java.lang.Exception
     */
    public static void main ( String[] args ) throws Exception {
        Connection conn = null;
        try {
            Class.forName( "org.sqlite.JDBC" ); // run the ClassLoader
            conn = DriverManager.getConnection( "jdbc:sqlite:test.sqlite" );
            _main( conn, true );
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if ( conn != null ) {
                conn.close();
            }
        }
    }

    @SuppressWarnings( "deprecation" )
    public static void _main ( Connection connection, boolean recreate ) throws Exception {

        Statement st = connection.createStatement();
        if ( recreate ) {
            st.executeUpdate( "\n"
                    + "DROP TABLE IF EXISTS `countries`;\n"
                    + "DROP TABLE IF EXISTS `cities`;\n"
                    + "DROP TABLE IF EXISTS `shops`;\n"
                    + "DROP TABLE IF EXISTS `products`;\n"
                    + "DROP TABLE IF EXISTS `productToShop`;\n"
                    + "DROP TABLE IF EXISTS `shopToCity`;\n" );
            st.execute( "-- 10/21/15 23:28:43\n"
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `countries`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `countries` (\n"
                    + "  `countryId` INT NOT NULL,\n"
                    + "  `countryName` VARCHAR(50) NOT NULL,\n"
                    + "  PRIMARY KEY (`countryId`)\n"
                    + ");\n" );
            st.execute( ""
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `cities`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `cities` (\n"
                    + " `cityId` INT NOT NULL,\n"
                    + " `cityName` VARCHAR(50) NOT NULL,\n"
                    + " `countryId` INT NOT NULL,\n"
                    + " PRIMARY KEY (`cityId`),\n"
                    //+ " INDEX `countryId_idx` (`countryId` ASC),\n"
                    + " CONSTRAINT `countryId`\n"
                    + " FOREIGN KEY (`countryId`)\n"
                    + " REFERENCES `countries` (`countryId`)\n"
                    + " ON DELETE NO ACTION\n"
                    + " ON UPDATE NO ACTION\n"
                    + ");\n" );
            st.execute( ""
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `shops`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `shops` (\n"
                    + " `shopId` INT NOT NULL,\n"
                    + " `shopName` VARCHAR(50) NOT NULL,\n"
                    + " PRIMARY KEY (`shopId`)\n"
                    + ");\n" );
            st.execute( ""
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `products`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `products` (\n"
                    + " `productId` INT NOT NULL,\n"
                    + " `productName` VARCHAR(50) NOT NULL,\n"
                    + " PRIMARY KEY (`productId`)\n"
                    + ");\n" );
            st.execute( ""
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `productToShop`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `productToShop` (\n"
                    + " `productToShopId` INT NOT NULL,\n"
                    + " `productId` INT NOT NULL,\n"
                    + " `shopId` INT NOT NULL,\n"
                    + " PRIMARY KEY (`productToShopId`),\n"
                    //+ " INDEX `productId_idx` (`productId` ASC),\n"
                    //+ " INDEX `shopId_idx` (`shopId` ASC),\n"
                    + " CONSTRAINT `productId`\n"
                    + " FOREIGN KEY (`productId`)\n"
                    + " REFERENCES `products` (`productId`)\n"
                    + " ON DELETE NO ACTION\n"
                    + " ON UPDATE NO ACTION,\n"
                    + " CONSTRAINT `shopId`\n"
                    + " FOREIGN KEY (`shopId`)\n"
                    + " REFERENCES `shops` (`shopId`)\n"
                    + " ON DELETE NO ACTION\n"
                    + " ON UPDATE NO ACTION\n"
                    + ");\n" );
            st.execute( ""
                    + "-- -----------------------------------------------------\n"
                    + "-- Table `shopToCity`\n"
                    + "-- -----------------------------------------------------\n"
                    + "CREATE TABLE IF NOT EXISTS `shopToCity` (\n"
                    + " `shopToCityId` INT NOT NULL,\n"
                    + " `shopId` INT NOT NULL,\n"
                    + " `cityId` INT NOT NULL,\n"
                    + " PRIMARY KEY (`shopToCityId`),\n"
                    //+ " INDEX `shopId_idx` (`shopId` ASC),\n"
                    //+ " INDEX `cityId_idx` (`cityId` ASC),\n"
                    + " CONSTRAINT `cityId`\n"
                    + " FOREIGN KEY (`cityId`)\n"
                    + " REFERENCES `cities` (`cityId`)\n"
                    + " ON DELETE NO ACTION\n"
                    + " ON UPDATE NO ACTION,\n"
                    + " CONSTRAINT `shopId`\n"
                    + " FOREIGN KEY (`shopId`)\n"
                    + " REFERENCES `shops` (`shopId`)\n"
                    + " ON DELETE NO ACTION\n"
                    + " ON UPDATE NO ACTION\n"
                    + ");\n"
            );

            PreparedStatement ps;

            ps = connection.prepareStatement( "INSERT INTO `countries` (`countryId`,`countryName`) VALUES (?, ?);" );
            int countryCount = 0;
            try (
                    FileReader fr = new FileReader( "countryList.txt" );
                    BufferedReader br = new BufferedReader( fr ); //
                    ) {
                while( br.ready() ) {
                    countryCount++;
                    ps.setInt( 1, countryCount );
                    ps.setString( 2, br.readLine() );
                    ps.addBatch();
                }
            }
            ps.executeBatch();

            Random rng = new Random();
            byte[] bytes = new byte[8];

            int entriesMax = 10000;

            ps = connection.prepareStatement( "INSERT INTO `cities` (`cityId`,`cityName`,`countryId`) VALUES (?, ?, ?);" );
            for( int i = 1; i <= entriesMax; i++ ) {
                rng.nextBytes( bytes );
                ps.setInt( 1, i );
                ps.setString( 2, "city of " + randomName( rng, bytes ) );
                ps.setInt( 3, rng.nextInt( countryCount ) + 1 );
                ps.addBatch();
            }
            ps.executeBatch();

            ps = connection.prepareStatement( "INSERT INTO `shops` (`shopId`,`shopName`) VALUES (?, ?);" );
            for( int i = 1; i <= entriesMax; i++ ) {
                rng.nextBytes( bytes );
                ps.setInt( 1, i );
                ps.setString( 2, "shop of " + randomName( rng, bytes ) );
                ps.addBatch();
            }
            ps.executeBatch();

            ps = connection.prepareStatement( "INSERT INTO `products` (`productId`,`productName`) VALUES (?, ?);" );
            for( int i = 1; i <= entriesMax; i++ ) {
                rng.nextBytes( bytes );
                ps.setInt( 1, i );
                ps.setString( 2, "product of " + randomName( rng, bytes ) );
                ps.addBatch();
            }
            ps.executeBatch();

            ps = connection.prepareStatement( "INSERT INTO `productToShop` (`productToShopId`,`productId`,`shopId`) VALUES (?, ?, ?);" );
            for( int i = 1; i <= entriesMax; i++ ) {
                rng.nextBytes( bytes );
                ps.setInt( 1, i );
                ps.setInt( 2, rng.nextInt( entriesMax ) + 1 );
                ps.setInt( 3, rng.nextInt( entriesMax ) + 1 );
                ps.addBatch();
            }
            ps.executeBatch();

            ps = connection.prepareStatement( "INSERT INTO `shopToCity` (`shopToCityId`,`shopId`,`cityId`) VALUES (?, ?, ?);" );
            for( int i = 1; i <= entriesMax; i++ ) {
                rng.nextBytes( bytes );
                ps.setInt( 1, i );
                ps.setInt( 2, rng.nextInt( entriesMax ) + 1 );
                ps.setInt( 3, rng.nextInt( entriesMax ) + 1 );
                ps.addBatch();
            }
            ps.executeBatch();
        }
        //System.out.println(countryCount);
        /*
         try ( ResultSet rs = st.executeQuery( "select * from countries" )) {
         while( rs.next() ) {
         System.out.println( rs.getInt( "countryId" ) + " " + rs.getString( "countryName" ) );
         }
         }
         */
        if(true)return;

        String select1 = "select `products`.`productName`,`shops`.`shopName`, `cities`.`cityName`, `countries`.`countryName` from `products`"
                + " inner join `productToShop` on `products`.`productId` = `productToShop`.`productId`"
                + " inner join `shops` on `productToShop`.`shopId` = `shops`.`shopId`"
                + " inner join `shopToCity` on `shops`.`shopId` = `shopToCity`.`shopId`"
                + " inner join `cities` on `shopToCity`.`cityId` = `cities`.`cityId`"
                + " inner join `countries` on `cities`.`countryId` = `countries`.`countryId`"
                + " where length(`productName`) < 15"
                + " order by length(`productName`), length(`shopName`), length(`cityName`), length(`countryName`)";
        String select2 = "select `products`.`productName`,`shops`.`shopName`, `cities`.`cityName`, `countries`.`countryName` from `products`"
                + " inner join `productToShop` on `products`.`productId` = `productToShop`.`productId`"
                + " inner join `shops` on `productToShop`.`shopId` = `shops`.`shopId`"
                + " inner join `shopToCity` on `shops`.`shopId` = `shopToCity`.`shopId`"
                + " inner join `cities` on `shopToCity`.`cityId` = `cities`.`cityId`"
                + " inner join `countries` on `cities`.`countryId` = `countries`.`countryId`"
                + " where `products`.`productId` in ( select `products`.`productId` from `products` where length(`productName`) < 15 )"
                + " order by length(`productName`), length(`shopName`), length(`cityName`), length(`countryName`)";
        String select3 = "select `shops`.`shopName`, `products`.`productName` from `products`"
                + " inner join `productToShop` on `products`.`productId` = `productToShop`.`productId`"
                + " inner join `shops` on `productToShop`.`shopId` = `shops`.`shopId`"
                + " where length(`productName`) < 15"
                + " order by length(`shopName`), length(`productName`)";
        String select4 = "select `shops`.`shopName`, `products`.`productName` from `products`, `productToShop`, `shops`"
                + " where `shops`.`shopId` = `productToShop`.`shopId` and `products`.`productId` = `productToShop`.`productId`"
                + "and length(`productName`) < 15"
                + " order by length(`shopName`), length(`productName`)";
        String select5 = "select `cities`.`cityName` from `cities`"
                + " inner join `countries` on `cities`.`countryId` = `countries`.`countryId`"
                + " where substr(`countryName`, 1, 1) = 'A'"
                + " order by `cityName`";
        String select6 = "select `cities`.`cityName` from `cities`"
                + " where `countryId` in"
                + " ( select `countries`.`countryId` from `countries` where substr(`countryName`, 1, 1) = 'A' )"
                + " order by `cityName`";

        query( st, select1, 1 );
        query( st, select2, 2 );
        query( st, select3, 3 );
        query( st, select4, 4 );
        query( st, select5, 5 );
        query( st, select6, 6 );
    }

    public static void query ( Statement st, String query, int i ) throws SQLException {
        System.out.println( "QUERY " + i + ": PLAN:" );
        try ( ResultSet rs = st.executeQuery( "EXPLAIN QUERY PLAN " + query ) ) {
            print( rs );
        }

        System.out.println( "QUERY " + i + ": QUERY:" );
        try ( ResultSet rs = st.executeQuery( query ) ) {
            System.out.println( print( rs ) + " records total" );
        }
    }

    public static int print ( ResultSet rs ) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int cc = rsmd.getColumnCount();
        int count = 0;

        while( rs.next() ) {
            for( int i = 1; i <= cc; i++ ) {
                rs.getString( i );
                //System.out.print( rs.getString( i ) );
                //System.out.print( '|' );
            }
            //System.out.println();
            count++;
        }
        return count;
    }

    public static String randomName ( Random rng, byte[] bytes ) {
        for( int i = bytes.length - 1; i >= 0; i-- ) {
            bytes[i] = (byte) ( rng.nextInt( 'z' - 'a' + 1 ) + 'a' );
        }
        bytes[0] = (byte) Character.toUpperCase( bytes[0] );
        return new String( bytes, 0, rng.nextInt( bytes.length ) + 1 );
    }
}
