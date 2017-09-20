/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bioinformaticproject;

import alignment.Distance;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import alignment.pairwise.MyPairwiseAlignment;

public class SequenceAligner {
    public static String align (ArrayList <String> pathes, String main){
        
        Comparator<String> stringLengthComparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.compare(o2.length(), o1.length());
            }
        };
        Collections.sort(pathes, stringLengthComparator);
        
        int mainLength = main.length();
        int pathLength = 0;
        int minDistancePos [] = new int [pathes.size()];
        StringBuilder output = new StringBuilder();
        output.append(main+"\n");
        int pathIndex = 0;
        for (String path : pathes){
            pathLength = path.length();
            if(path.length() >= 0.85 * mainLength){
                output.setLength(0);
                String pairwise = new MyPairwiseAlignment().align(path, main); 
                output.append(pairwise + "\n");
                continue;
            }
            
            if (pathLength < mainLength){
                int score[] = new int [mainLength - pathLength + 1];
                for (int i=0 ; i< score.length; i++){
                    //System.out.print("\n"+path+ "   "+main.substring(i, i+pathLength));
                    score [i] = Distance.getHammingDistance(path, main.substring(i, i + pathLength));
                    //System.out.print("  "+score [i]);
                }
                //findin min in score array
                int min = mainLength;
                int pos = 0;
                for (int j=0 ; j < score.length ; j++){
                    if (min > score [j]){
                        min = score[j];
                        pos = j;
                    }
                }
                minDistancePos [pathIndex]= pos;
            }
            else {
                
            }
            pathIndex ++;
        }
        
        for (int i=0 ; i < pathes.size(); i++){
            if(pathes.get(i).length()==0){
                continue;
            }
            for(int j=0 ; j < minDistancePos [i] ; j++ ){
                output.append(" ");
            }
            output.append(pathes.get(i) + "\n");
            
        }
        return output.toString();
    }
    
    public static void main(String[] args) {
        System.out.println("\n"+align(new ArrayList<String> (Arrays.asList("AAG","CACA","CACGAAG","GTGATAT","AGTGGCAC","TTCCGGT","ATT","AAGCACTATTAGTTCGTTT")),
                "ATTCGTGGCACTATTAGTGATAGTTCCGGCACACGAAGCC"));
    }
}
