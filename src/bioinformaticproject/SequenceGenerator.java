/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bioinformaticproject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class SequenceGenerator {
    public static String generateSequence (int aPercent, int cPercent, int tPercent, int gPercent, int length){
        int a=(int) (aPercent*length)/100;
        int t= (int)(tPercent*length)/100;
        int c= (int)(cPercent*length)/100;
        int g= length-(a+t+c);
        ArrayList <Character> options = new ArrayList<> ();
        if(a > 0){options.add('A');}
        if(t > 0){options.add('C');}
        if(c > 0){options.add('T');}
        if(g > 0){options.add('G');}
        
        Random r=new Random();
        char random;
        StringBuilder sequence=new StringBuilder();
        for (int i=0 ; i< length ; i++){
            random=options.get(r.nextInt(options.size()));
            switch (random){
                case 'A':
                        sequence.append(random);
                        a--;
                        if(a<=0){
                            options.remove(options.indexOf('A'));
                        }
                        break;
                    case 'T':
                        sequence.append(random);
                        t--;
                        if(t<=0){
                            options.remove(options.indexOf('T'));
                        }
                        break;
                    case 'C':
                        sequence.append(random);
                        c--;
                        if(c<=0){
                            options.remove(options.indexOf('C'));
                        }
                        break;
                    case 'G':
                        sequence.append(random);
                        g--;
                        if(g<=0){
                            options.remove(options.indexOf('G'));
                        }
                        break;
                    
            }
        }
        return sequence.toString();
    }
    
    
    
    
}
