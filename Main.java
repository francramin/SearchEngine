/**
 * Created by francisco on 17/11/16.
 */
public class Main
{
    static Controller miController;

    public static void main(String[] args) throws Exception
    {
        miController = new Controller();

        View v = new View(miController);
        v.setVisible(true);
        v.setDefaultCloseOperation(v.EXIT_ON_CLOSE);


        miController.conectDB();

        //comentar/descomentar
        /*
        miController.createTables();

        System.out.println("adipobiology");
        miController.parserXML("adipobiology.xml");

        System.out.println("electromagnetics");
        miController.parserXML("advanced-electromagnetics.xml");

        System.out.println("economics");
        miController.parserXML("advances-in-applied-economics-and-finance.xml");

        miController.updateIdf();
        miController.updateAppearance();
        */


        System.out.println("Termine");






    }

}
