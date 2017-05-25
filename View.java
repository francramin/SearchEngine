/**
 * Created by francisco on 5/09/16.
 */
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class View extends JFrame {
    Button actualizarDB;
    Button consultar;



    TextField textFQuery;


    TextArea resultado;




    Label queryCosine;


    Controller miCollection;


    //Constructor de la clase View
    public View(Controller unaCollection) {
        Color color;
        setBounds(200, 50, 900, 500);
        setLayout(null);
        componentes();

        miCollection = unaCollection;



        actualizarDB.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                miCollection.updateIdf();
                miCollection.updateAppearance();

            }
        });
        consultar.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String resultados = "";
                resultados = miCollection.doQuery(textFQuery.getText());
                resultado.setText(resultados);


            }
        });




    }//end constructor

    //Componentes dentro de la interfaz del view
    private void componentes(){
        ////////////////////////////////////////LABELS///////////////////////////

        queryCosine = new Label("Buscar :");
        queryCosine.setBounds(30, 80, 50, 20);
        add(queryCosine);


        /////////////////////////////////////////////Botones//////////////////////////////////////////////


        actualizarDB = new Button("Actualizar IDF");
        actualizarDB.setBounds(680, 80, 90, 20);
        add(actualizarDB);

        consultar = new Button("Buscar");
        consultar.setBounds(580, 80, 70, 20);
        add(consultar);

         /////////////////////////////TEXT  FIELD//////////////////////////////////////


        textFQuery = new TextField();
        textFQuery.setEditable(true);
        textFQuery.setBounds(100, 80, 450, 20);
        add(textFQuery);

         ////////////////////////////////////////// TEXT AREAS //////////////////////
        resultado = new TextArea();
        resultado.setEditable(false);
        resultado.setBounds(53, 130, 800, 150);
        add(resultado);





    }//end metodo componentes

}