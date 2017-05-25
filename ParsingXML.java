/**
 * Created by francisco on 17/11/16.
 */
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import java.io.*;

public abstract class ParsingXML{

    public static NodeList parseXML(String xmldoc) throws ParserConfigurationException, IOException, SAXException {
        String filepath = System.getProperty("user.dir");
        File xmltoparse = new File(filepath +"/"+ xmldoc);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(xmltoparse);
        doc.getDocumentElement().normalize();
        return doc.getElementsByTagName("oai_dc:dc");
    }
}
