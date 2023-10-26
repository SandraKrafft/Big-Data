package tn.bigdata.tp1;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

import static java.lang.Integer.parseInt;

public class GetTotSalesMapper extends Mapper<Object, Text, Text, FloatWritable> { //ENTREE == Objet ->type text || SORTIE Nom Magasin & Ventes

    private Text Magasin = new Text();
    private FloatWritable ventes = new FloatWritable();
    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] colonnes = value.toString().split("\t"); //Les colonnes sont séparés par des tabulations
        //StringTokenizer itr = new StringTokenizer(value.toString());
        if(colonnes.length >= 4) {
            if(colonnes[4].length() <1)
            {
                Magasin.set("Magasin non renseigné\n");
                Float revenu = Float.parseFloat(colonnes[4]);
                ventes.set(revenu);
                context.write(Magasin, ventes);
            }
            else {

                String nomMag = colonnes[2];
                Float revenu = Float.parseFloat(colonnes[4]);
                Magasin.set(nomMag);
                ventes.set(revenu);
                context.write(Magasin, ventes);
            }
        }

    }
}
