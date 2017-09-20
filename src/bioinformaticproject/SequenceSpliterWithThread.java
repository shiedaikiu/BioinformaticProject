/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bioinformaticproject;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SequenceSpliterWithThread extends Thread{

    private String sequence;
    private int min,max,copy;
    private OnProgressListener onProgressListener;
    private OnProgressFinishListener onProgressFinishListener;

    public void setOnProgressFinishListener(OnProgressFinishListener onProgressFinishListener) {
        this.onProgressFinishListener = onProgressFinishListener;
    }
    

    public void setOnProgressListener(OnProgressListener onProgressListener) {
        this.onProgressListener = onProgressListener;
    }

    public SequenceSpliterWithThread(String sequence, int min, int max, int copy) {
        this.sequence = sequence;
        this.min = min;
        this.max = max;
        this.copy = copy;
    }
    
    
    @Override
    public void run() {
        ArrayList<String> MainResult = new ArrayList<>();
        for (int i = 0; i < copy; i++) {
            int start = 0;
            int end = 0;
            int length = sequence.length();
            ArrayList<String> subResults = new ArrayList<>();
            
            Random random = new Random();
            int randomLength;
            while (start < length) {
                randomLength = (random.nextInt(max - min + 1)) + min;
                end = start + randomLength;
                String temp;
                if (end > length) {
                    end = length;
                }
                
                temp = sequence.substring(start, end);
                start = end;
                subResults.add(temp);
            }
            MainResult.addAll(subResults);
            if(onProgressListener!=null){
                onProgressListener.onProgress(((i+1)*100)/copy);
            }
            subResults.clear();
        }
       onProgressFinishListener.onProgressFinish(MainResult);
    }
    
    public interface OnProgressListener {
        public void onProgress (int progress);
    }
    
    public interface OnProgressFinishListener{
        public void onProgressFinish (ArrayList<String> slices);
    }
}
