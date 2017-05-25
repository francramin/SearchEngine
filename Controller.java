import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.Vector;

/**
 * Created by francisco on 17/11/16.
 */
public class Controller {

    static Vector<String> documentV;
    static Vector<String> termV = new Vector();

    static Connection conn = null;
    static Statement stmt = null;
    static ResultSet rs = null;

    static int metacounter = 1;

    public static final String[] metadataTables = {"consulta","contributor","coverage",
            "creator","date","description","document","document_has_term","format",
            "identifier","language","publisher","relation","rights","source",
            "subject","term","type"};



    public void conectDB() {
        try {
            // The newInstance() call is a work around for some
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/OAI", "root", "hola123");
            //  (url/nombredelaDB,user,password)
        } catch (Exception ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }

    }

    public void createTables() {
        try {
            String query = "drop table if exists dochasterm";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "drop table if exists document";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "drop table if exists term";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();


            query = "CREATE TABLE IF NOT EXISTS " + "term"
                    + "(idterm  VARCHAR(30) primary key, "
                    + " indocappearances int(4)," +
                    "idf float);";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "document"
                    + "(iddoc  int(4) primary key, "
                    + "text VARCHAR(1000));";


            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "dochasterm"
                    + "(iddoc  int(4), "
                    + " idterm VARCHAR(30), "
                    + " appearances int(4),"
                    + " weight float(4),"
                    + "foreign key (iddoc) references document (iddoc),"
                    + "foreign key (idterm) references term (idterm));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "rights"
                    + "(iddoc  int(4), "
                    + "rights text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();


            query = "CREATE TABLE IF NOT EXISTS " + "relation"
                    + "(iddoc  int(4), "
                    + " relation text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();


            query = "CREATE TABLE IF NOT EXISTS " + "subject"
                    + "(iddoc  int(4), "
                    + " subject text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();


            query = "CREATE TABLE IF NOT EXISTS " + "date"
                    + "(iddoc  int(4), "
                    + " date text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "coverage"
                    + "(iddoc  int(4), "
                    + "coverage text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "identifier"
                    + "(iddoc  int(4), "
                    + "identifier text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "contributor"
                    + "(iddoc  int(4), "
                    + "contributor text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "language"
                    + "(iddoc  int(4), "
                    + "lenguage text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "type"
                    + "(iddoc  int(4), "
                    + " type text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "publisher"
                    + "(iddoc  int(4), "
                    + " publisher text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "description"
                    + "(iddoc  int(4), "
                    + "description text,"
                    + "foreign key (iddoc) references document (iddoc));";

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "source"
                    + "(iddoc  int(4), "
                    + " source text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS " + "format"
                    + "(iddoc  int(4), "
                    + "format text,"
                    + "foreign key (iddoc) references document (iddoc));";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
            query = "CREATE TABLE IF NOT EXISTS " + "creator"
                    + "(iddoc  int(4), "
                    + "creator text,"
                    + "foreign key (iddoc) references document (iddoc));";

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            System.out.println("Created tables");

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        // Now do something with the ResultSet ....
    }

    public void readDoc(int doc, Vector<String> unVector) {
        Collections.sort(unVector);
        int i = 0;
        String query = "insert ignore into dochasterm(iddoc,idterm,appearances) values";
        int repeat = 1;
        while (i < unVector.size()) {

            try {
                if (i == unVector.size() - 1) {
                    query = query + " (" + doc + ",'" + unVector.get(i) + "'," + repeat + "),";
                    repeat = 1;
                } else if ((unVector.get(i).equals(unVector.get(i + 1)))) {
                    repeat++;
                } else {
                    query = query + " (" + doc + ",'" + unVector.get(i) + "'," + repeat + "),";
                    repeat = 1;
                }

            } catch (Exception e) {
            }

            i++;
        }

        query = query.substring(0, query.length() - 1);
        query = query + ";";
        System.out.println(query);

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    public void addTerms2(Vector unV)
    {

        int i=0;
        String query= "insert ignore into term(idterm) values " ;
        System.out.println(unV.size());
        while(i<unV.size())
        {
            query = query + "('" + unV.get(i) + "')," ;
            i++;

        }

        query =  query.substring(0, query.length() - 1);
        query = query + ";";
        System.out.println(query);

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
        }
        catch (SQLException ex){
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

    }

    public void addDoc(int something, String untexto) {
        try {
            stmt = conn.createStatement();
            String query = "";
            int i=0;
            if(something==0)
                ;


            query = "insert ignore into " + "document"
                    + "(iddoc,text) values("
                    + something + ",'"+ untexto +"')";
            System.out.println(query);
            stmt.executeUpdate(query);
            stmt.close();
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

    }

    public void updateAppearance() {
        termV = new Vector<>();
        Vector<Integer> total  = new Vector<Integer>();
        String query = "";
        try {

            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT idterm FROM term;");

            while (rs.next()) {
                String str = rs.getString("idterm");
                termV.add(str);
            }
            stmt.close();
        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        int i = 0;
        while (i < termV.size()) {
            total.add(setAppearance(termV.get(i)));
            i++;
        }

        System.out.println(total.size());
        System.out.println(termV.size());

        i=0;
        query = "update term set indocappearances= case";
        while(i < termV.size())
        {
            query = query + " when idterm = '" + termV.get(i) + "' then " + total.get(i);
            i++;
        }
        query = query +" end;";

        System.out.println(query);
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

        System.out.println("Se actualizó las apariciones");

    }

    public int setAppearance(String term)
    {

        int docs = 0;
        try {


            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT count(*) as totales FROM dochasterm WHERE idterm = '" + term + "';");

            if (rs.next())
                docs = rs.getInt("totales");

            System.out.println(term + "," + docs);

            //query = "update term set indocappearances = " + docs + " where idterm = '" + term + "';";
            //stmt.executeUpdate(query);

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
        }
        return docs;
    }

    public void updateIdf() {

        float idf = 0;
        String term  = "";
        Vector<Float> idfV = new Vector<Float>();
        Vector termV = new Vector();
        String query = "";
        try {

            int numb = 0;
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select count(*) as suma from document");

            while(rs.next())
            {
                numb = rs.getInt("suma");
            }

            stmt.close();

            stmt = conn.createStatement();
            rs = stmt.executeQuery("select idterm, log10("+numb+"/count(*)) as idf from dochasterm group by idterm;");

            while (rs.next()) {
                idf = rs.getFloat("idf");
                idfV.add(idf);
                term = rs.getString("idterm");
                termV.add(term);
            }

            stmt.close();

            int i=0;
            query = "update term set idf= case";
            while(i < idfV.size())
            {
                query = query + " when idterm = '" + termV.get(i) + "' then " + idfV.get(i);
                i++;
            }
            query = query +" end;";

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
        }

        System.out.println(query);
        System.out.println("Se actualizó las idf");;

    }

    public String doQuery(String aQuery)
    {
        Vector queryTerms = new Vector();
        try {
            String query = "drop table if exists query";
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            query = "CREATE TABLE IF NOT EXISTS query(idterm varchar(30), tf int(4));";

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();


        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

        Scanner sc2 = new Scanner(aQuery);
        Vector unVector = new Vector();

        while (sc2.hasNextLine()) {
            Scanner s2 = new Scanner(sc2.nextLine());
            String s = "";

            while (s2.hasNext()) {

                s = s2.next();
                queryTerms.add(s);
            }
        }

        Collections.sort(queryTerms);

        String query = "insert ignore into query(idterm, tf) values";
        int repeat = 1;
        int i=0;
        while (i < queryTerms.size()) {

            try {
                if (i == queryTerms.size()-1) {
                    query = query + "('" + queryTerms.get(i) + "'," + repeat + "),";
                    repeat = 1;
                } else if ((queryTerms.get(i).equals(queryTerms.get(i + 1)))) {
                    repeat++;
                } else {
                    query = query + " ('" +queryTerms.get(i) + "'," + repeat + "),";
                    repeat = 1;
                }

            } catch (Exception e) {}

            i++;
        }
        query = query.substring(0, query.length() - 1);
        query = query + ";";
        String result = "";
        System.out.println(queryTerms.size());
        System.out.println(query);
        try
        {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();

            stmt = conn.createStatement();
            query = "select i.iddoc, sum(q.tf * t.idf * i.appearances * t.idf) as suma from query q, dochasterm i, term t  where q.idterm = t.idterm" +
                    " AND i.idterm = t.idterm  group by i.Iddoc order by 2 desc";
            rs =  stmt.executeQuery(query);

            while (rs.next()) {
                String doc = rs.getString("iddoc");
                float suma = rs.getFloat("suma");
                result = result + " ID----" + doc  + "    PESO-----" + suma  +"\n";
            }

            stmt.close();

        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }


        return result;


    }


    public void parserXML(String xmlToParse) throws IOException, SAXException, ParserConfigurationException
    {
        NodeList nodeList = ParsingXML.parseXML(xmlToParse);
        String titulo = null;
        for(int i=0;i<nodeList.getLength();i++)
        { //Guardamos los nodos oai_dc:dc
            Node node = nodeList.item(i);
            NodeList nodes = node.getChildNodes(); //Obtenemos los hijos con tags como 'title', 'author', ...
            ArrayList<String[]> metalist = new ArrayList<String[]>();
            for(int j=0;j<nodes.getLength();j++) {
                Node node2 = nodes.item(j);
                if(node2.getNodeType()== Node.ELEMENT_NODE)
                {
                    //System.out.println(node2.getNodeName());
                    String temptext = node2.getNodeName().substring(3); //-'dc:'
                    if(!temptext.equalsIgnoreCase("title"))
                    {
                        String[] metadatos = new String[2];
                        metadatos[0] = temptext;
                        metadatos[1] = node2.getTextContent(); //Extraemos el texto del elemento
                        metalist.add(metadatos);
                    }
                    else
                    {
                        titulo = node2.getTextContent();
                    }
                    //System.out.println(node2.getTextContent());
                }
            }
            titulo = titulo.replaceAll("'","");
            insertarDocMeta(metacounter, titulo, metalist);
            metacounter++;
        }
    }

    public void insertarDocMeta(int iddoc, String titulo, ArrayList<String[]> metalist) {
        addDoc(iddoc, titulo);
        String supermegatext = " ";
        Statement st = crearStatement();
        for(int i = 0; i<metalist.size(); i++){
            String metadata[] = metalist.get(i);
            for(int j = 0; j<metadataTables.length; j++ ) {
                if(metadataTables[j].equalsIgnoreCase(metadata[0]) && !metadata[1].contentEquals("")) {
                    metadata[1] = metadata[1].replaceAll("'", "''"); //Evitar errores de sintaxis relacionados con (')
                    supermegatext = supermegatext + " " + metadata[1];
                    String temporal = "(" + iddoc + " , '" + metadata[1] + "');";
                    smallInsert(st, metadata[0],temporal);
                    j = metadataTables.length+1;
                }
            }
        }
        //supermegatext = supermegatext.replaceAll("\\s", " ");
        //supermegatext = supermegatext.replaceAll("\\s+", " ");

        String[] s = supermegatext.split("\\s+");
        int i =0;
        while(i < s.length)
        {
            s[i] =  s[i].replaceAll("'","");
            termV.add(s[i]);
            i++;
        }

        addTerms2(termV);
        readDoc(metacounter,termV);
        termV = new Vector();
    }

    public void smallInsert(Statement st, String table, String ordVal) {
        try {
            System.out.println("INSERT ignore INTO " + table +" VALUES " + ordVal);
            st.executeUpdate("INSERT ignore INTO "+ table +" VALUES " + ordVal);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Statement crearStatement() {
        Statement st = null;
        try{
            st = conn.createStatement();

        } catch (SQLException e) {
            System.err.println("failed");
            e.printStackTrace(System.err);
        }
        return st; //Regresará un valor nulo si ocurre un error
    }

    public String[][] calcularSimPtP(){
        ResultSet rs = null;
        String[][] resultados = new String[1][1];
        try {
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM document;");
            rs.next();
            int docCount = rs.getInt("COUNT(*)");
            rs = null;
            resultados = new String[docCount][16];
            rs = conn.createStatement().executeQuery("SELECT similitud.iddoc, similitud.sim, do.text, cov.coverage, cr.creator, da.date, des.description,\n" +
                    "fo.format,ide.identifier, lan.lenguage, pub.publisher, rel.relation,\n" +
                    "rig.rights, sou.source, sub.subject,typ.type, co.contributor\n" +
                    "FROM (select i.iddoc, sum(q.tf * t.idf * i.appearances * t.idf) AS sim, d.text\n" +
                    "from query q, dochasterm i,document d, term t\n" +
                    "where q.idterm = t.idterm AND i.idterm = t.idterm AND d.iddoc = i.iddoc\n" +
                    "group by i.iddoc order by 2 desc) as similitud \n" +
                    "LEFT JOIN document do on do.iddoc = similitud.iddoc\n" +
                    "LEFT JOIN contributor co on do.iddoc = co.iddoc\n" +
                    "LEFT JOIN coverage cov on cov.iddoc = do.iddoc\n" +
                    "LEFT JOIN creator cr on cr.iddoc = do.iddoc\n" +
                    "LEFT JOIN date da on da.iddoc = do.iddoc\n" +
                    "LEFT JOIN description des on des.iddoc = do.iddoc\n" +
                    "LEFT JOIN format fo on fo.iddoc = do.iddoc\n" +
                    "LEFT JOIN identifier ide on ide.iddoc = do.iddoc\n" +
                    "LEFT JOIN language lan on lan.iddoc = do.iddoc\n" +
                    "LEFT JOIN publisher pub on pub.iddoc = do.iddoc\n" +
                    "LEFT JOIN relation rel on rel.iddoc = do.iddoc\n" +
                    "LEFT JOIN rights rig on rig.iddoc = do.iddoc\n" +
                    "LEFT JOIN source sou on sou.iddoc = do.iddoc\n" +
                    "LEFT JOIN subject sub on sub.iddoc = do.iddoc\n" +
                    "LEFT JOIN type typ on typ.iddoc = do.iddoc\n" +
                    "group by similitud.iddoc order by 2 desc;");
            for (int i = 0; i < docCount; i++) {
                if (!rs.isLast() && rs.next()) {
                    int id = rs.getInt("document_id");
                    Float sim = rs.getFloat("sim");
                    String titulo = rs.getString("identifier");
                    resultados[i][0] = Integer.toString(id);
                    resultados[i][1] = Float.toString(sim);
                    resultados[i][2] = titulo;
                    resultados[i][3] = rs.getString("coverage");
                    resultados[i][4] = rs.getString("contributor");
                    resultados[i][5] = rs.getString("creator");
                    resultados[i][6] = rs.getString("date");
                    resultados[i][7] = rs.getString("description");
                    resultados[i][8] = rs.getString("format");
                    resultados[i][9] = rs.getString("language");
                    resultados[i][10] = rs.getString("publisher");
                    resultados[i][11] = rs.getString("relation");
                    resultados[i][12] = rs.getString("rights");
                    resultados[i][13] = rs.getString("source");
                    resultados[i][14] = rs.getString("subject");
                    resultados[i][15] = rs.getString("type");
                } else {
                    i = docCount;
                }
            }
        }
        catch (SQLException ex)
        {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return resultados;
    }
}
