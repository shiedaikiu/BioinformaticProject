/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bioinformaticproject;

import com.philiphubbard.sabe.BasicAssembler;
import java.util.ArrayList;

public class SequenceAssembler {
    public static ArrayList<String> assembleSequence (ArrayList <String> slices){
        ArrayList <String> pathes;
        BasicAssembler assembler = new BasicAssembler(slices, 3, false);
        pathes = assembler.assemble();
        return pathes;
    }
}
