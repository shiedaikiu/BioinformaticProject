/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bioinformaticproject;

import java.util.ArrayList;
import java.util.Random;

public class SequenceSpliter {
    public static ArrayList<String> split(String sequence,int min, int max,  int copy) {
        ArrayList<String> reads = new ArrayList<>();
        for (int i = 0; i < copy; i++) {
            int left = 0;
            int right = 0;
            int l = sequence.length();
            ArrayList<String> tempReads = new ArrayList<>();
            Random random = new Random();
            int randomLength;
            while (left < l) {
                randomLength = (random.nextInt(max - min + 1)) + min;
                right = left + (randomLength);
                String tmp;
                if (right > l || (l - right) < min) {
                    right = l;
                }
                tmp = sequence.substring(left, right);
                left = right;
                tempReads.add(tmp);
            }
            reads.addAll(tempReads);
            tempReads.clear();
        }
        return reads;
    }
}
